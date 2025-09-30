;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at

;;     http://www.apache.org/licenses/LICENSE-2.0

;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns metabase.driver.implementation.sync
  "Sync implementation for Starburst driver."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [java-time :as t]
            [metabase.driver :as driver]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.sync.describe-database :as sql-jdbc.describe-database]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql-jdbc.sync.interface :as sql-jdbc.sync.interface]
            [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
            [metabase.driver.sql.util :as sql.u]
            [metabase.util.i18n :refer [trs]])
  (:import (java.sql Connection)))

(def starburst-type->base-type
  "Function that returns a `base-type` for the given `straburst-type` (can be a keyword or string)."
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [[#"(?i)boolean"                    :type/Boolean]
    [#"(?i)tinyint"                    :type/Integer]
    [#"(?i)smallint"                   :type/Integer]
    [#"(?i)integer"                    :type/Integer]
    [#"(?i)bigint"                     :type/BigInteger]
    [#"(?i)real"                       :type/Float]
    [#"(?i)double"                     :type/Float]
    [#"(?i)decimal.*"                  :type/Decimal]
    [#"(?i)varchar.*"                  :type/Text]
    [#"(?i)char.*"                     :type/Text]
    [#"(?i)varbinary.*"                :type/*]
    [#"(?i)json"                       :type/Text]
    [#"(?i)date"                       :type/Date]
    [#"(?i)^timestamp$"                :type/DateTime]
    [#"(?i)^timestamp\(\d+\)$"         :type/DateTime]
    [#"(?i)^timestamp with time zone$" :type/DateTimeWithTZ]
    [#"(?i)^timestamp with time zone\(\d+\)$" :type/DateTimeWithTZ]
    [#"(?i)^timestamp\(\d+\) with time zone$" :type/DateTimeWithTZ]
    [#"(?i)^time$"                     :type/Time]
    [#"(?i)^time\(\d+\)$"              :type/Time]
    [#"(?i)^time with time zone$"      :type/TimeWithTZ]
    [#"(?i)^time with time zone\(\d+\)$"  :type/TimeWithTZ]
    [#"(?i)^time\(\d+\) with time zone$"  :type/TimeWithTZ]
    [#"(?i)array"                      :type/Array]
    [#"(?i)map"                        :type/Dictionary]
    [#"(?i)row.*"                      :type/*]
    [#".*"                             :type/*]]))

(defn describe-catalog-sql
  "The SHOW SCHEMAS statement that will list all schemas for the given `catalog`."
  {:added "0.39.0"}
  [driver catalog]
  (str "SHOW SCHEMAS FROM " (sql.u/quote-name driver :database catalog)))

(defn describe-schema-sql
  "The SHOW TABLES statement that will list all tables for the given `catalog` and `schema`."
  {:added "0.39.0"}
  [driver catalog schema]
  (str "SHOW TABLES FROM " (sql.u/quote-name driver :schema catalog schema)))

(defn describe-table-sql
  "The DESCRIBE  statement that will list information about the given `table`, in the given `catalog` and schema`."
  {:added "0.39.0"}
  [driver catalog schema table]
  (str "DESCRIBE " (sql.u/quote-name driver :table catalog schema table)))

(def excluded-schemas
  "The set of schemas that should be excluded when querying all schemas."
  #{"information_schema"})

(defmethod sql-jdbc.sync/database-type->base-type :starburst
  [_ field-type]
  (let [base-type (starburst-type->base-type field-type)]
    (log/debugf "database-type->base-type %s -> %s" field-type base-type)
    base-type))

(defn show-create-table-sql
  "The DESCRIBE  statement that will list information about the given `table`, in the given `catalog` and schema`."
  {:added "0.39.0"}
  [driver catalog schema table]
  (str "SHOW CREATE TABLE " (sql.u/quote-name driver :table catalog schema table)))

(defmethod sql-jdbc.sync.interface/have-select-privilege? :starburst
  [driver ^Connection conn table-schema table-name]
  (try
    (let [sql (str "SHOW TABLES FROM \"" table-schema "\" LIKE '" table-name "'")]
      (log/debugf "Checking select privilege for table: schema='%s', table='%s' with query: %s" 
                  table-schema table-name sql)
      ;; if the query completes without throwing an Exception, we can SELECT from this table
      (with-open [stmt (.prepareStatement conn sql)
                  rs (.executeQuery stmt)]
          (let [has-privilege (.next rs)]
            (log/debugf "Select privilege check result for table: schema='%s', table='%s' = %s" 
                        table-schema table-name has-privilege)
            has-privilege)))
    (catch Throwable e
      (log/warnf e "Failed to check select privilege for table: schema='%s', table='%s': %s" 
                 table-schema table-name (.getMessage e))
      false)))

(defn- describe-schema-attempt
  "Single attempt to get tables for a schema."
  [driver conn catalog schema attempt max-retries]
  (let [sql (describe-schema-sql driver catalog schema)]
    (try
      (log/infof "Attempt %d/%d: Executing describe-schema query for catalog='%s', schema='%s'" 
                 attempt max-retries catalog schema)
      
      ;; Execute the query and process results in one go to avoid ResultSet closure issues
      (let [result (with-open [stmt (.createStatement conn)]
                     (let [rs (sql-jdbc.execute/execute-statement! driver stmt sql)]
                       (log/infof "Successfully executed describe-schema query for catalog='%s', schema='%s' on attempt %d" 
                                  catalog schema attempt)
                       ;; Process the ResultSet immediately before it gets closed
                       ;; Skip privilege checking for now to avoid timeout issues
                       (into 
                        #{} 
                        (map (fn [{table-name :table}]
                               {:name        table-name
                                :schema      schema})) 
                        (jdbc/reducible-result-set rs {}))))]
        (log/infof "Successfully processed %d tables for catalog='%s', schema='%s' on attempt %d" 
                   (count result) catalog schema attempt)
        result)
      
      (catch Throwable e
        (log/warnf e "Attempt %d/%d failed for describe-schema catalog='%s', schema='%s': %s" 
                   attempt max-retries catalog schema (.getMessage e))
        
        (if (< attempt max-retries)
          (let [delay (long (* 10000 (Math/pow 2 (dec attempt))))] ; 10s, 20s, 40s
            (log/infof "Retrying in %.1f seconds... (attempt %d/%d)" (/ delay 1000.0) (inc attempt) max-retries)
            (Thread/sleep delay)
            (describe-schema-attempt driver conn catalog schema (inc attempt) max-retries))
          (do
            (log/errorf e "All %d attempts failed for describe-schema catalog='%s', schema='%s'. Giving up." 
                       max-retries catalog schema)
            (throw e)))))))

(defn- describe-schema-with-retry
  "Gets a set of maps for all tables in the given `catalog` and `schema` with retry logic."
  [driver conn catalog schema max-retries]
  (log/infof "Starting describe-schema for catalog='%s', schema='%s' with %d retries" catalog schema max-retries)
  (describe-schema-attempt driver conn catalog schema 1 max-retries))

(defn- describe-schema
  "Gets a set of maps for all tables in the given `catalog` and `schema`."
  [driver conn catalog schema]
  (describe-schema-with-retry driver conn catalog schema 3))

(defn- all-schemas-attempt
  "Single attempt to get all schemas for a catalog."
  [driver conn catalog attempt max-retries]
  (let [sql (describe-catalog-sql driver catalog)]
    (try
      (log/infof "Attempt %d/%d: Executing all-schemas query for catalog='%s'" 
                 attempt max-retries catalog)
      
      ;; Execute the query and process results in one go to avoid ResultSet closure issues
      (let [result (with-open [stmt (.createStatement conn)]
                     (let [rs (sql-jdbc.execute/execute-statement! driver stmt sql)]
                       (log/infof "Successfully executed all-schemas query for catalog='%s' on attempt %d" 
                                  catalog attempt)
                       ;; Process the ResultSet immediately before it gets closed
                       (into []
                             (map (fn [{:keys [schema] :as full}]
                                    (when-not (contains? excluded-schemas schema)
                                      (describe-schema driver conn catalog schema))))
                             (jdbc/reducible-result-set rs {}))))]
        (log/infof "Successfully processed %d schemas for catalog='%s' on attempt %d" 
                   (count result) catalog attempt)
        result)
      
      (catch Throwable e
        (log/warnf e "Attempt %d/%d failed for all-schemas catalog='%s': %s" 
                   attempt max-retries catalog (.getMessage e))
        
        (if (< attempt max-retries)
          (let [delay (long (* 10000 (Math/pow 2 (dec attempt))))] ; 10s, 20s, 40s
            (log/infof "Retrying in %.1f seconds... (attempt %d/%d)" (/ delay 1000.0) (inc attempt) max-retries)
            (Thread/sleep delay)
            (all-schemas-attempt driver conn catalog (inc attempt) max-retries))
          (do
            (log/errorf e "All %d attempts failed for all-schemas catalog='%s'. Giving up." 
                       max-retries catalog)
            (throw e)))))))

(defn- all-schemas-with-retry
  "Gets a set of maps for all tables in all schemas in the given `catalog` with retry logic."
  [driver conn catalog max-retries]
  (log/infof "Starting all-schemas for catalog='%s' with %d retries" catalog max-retries)
  (all-schemas-attempt driver conn catalog 1 max-retries))

(defn- all-schemas
  "Gets a set of maps for all tables in all schemas in the given `catalog`."
  [driver conn catalog]
  (all-schemas-with-retry driver conn catalog 3))
  
(defmethod driver/describe-database :starburst
  [driver {{:keys [catalog schema] :as details} :details :as database}]
  (log/infof "Starting describe-database for Starburst: catalog='%s', schema='%s'" catalog schema)
  (sql-jdbc.execute/do-with-connection-with-options
    driver
    database
    nil
    (fn [^Connection conn]
      (log/infof "Connection established for describe-database: catalog='%s', schema='%s'" catalog schema)
      (let [schemas (if schema 
                      (do
                        (log/infof "Syncing specific schema: '%s' for catalog: '%s'" schema catalog)
                        #{(describe-schema driver conn catalog schema)})
                      (do
                        (log/infof "Syncing all schemas for catalog: '%s'" catalog)
                        (all-schemas driver conn catalog)))]
        (log/infof "Completed describe-database for Starburst: catalog='%s', schema='%s', found %d schemas" 
                   catalog schema (count schemas))
        {:tables (reduce set/union schemas)}))))

(defmethod driver/describe-table :starburst
  [driver {{:keys [catalog] :as details} :details :as database} {schema :schema, table-name :name}]
  (sql-jdbc.execute/do-with-connection-with-options
    driver
    database
    nil
    (fn [^Connection conn]
      (with-open [stmt (.createStatement conn)]
        (let [sql (describe-table-sql driver catalog schema table-name)
              rs (sql-jdbc.execute/execute-statement! driver stmt sql)]
          (println "--------describe sql------------")
          (println sql)
          {:schema schema
           :name   table-name
           :fields (into
            #{}
            (map-indexed (fn [idx {:keys [column type] :as col}]
                            {:name column
                            :database-type type
                            :base-type         (starburst-type->base-type type)
                            :database-position idx}))
            (jdbc/reducible-result-set rs {}))})))))

(defmethod driver/db-default-timezone :starburst
  [driver {{:keys [catalog] :as details} :details :as database}]
  (sql-jdbc.execute/do-with-connection-with-options
    driver
    database
    nil
    (fn [^Connection conn]
      (with-open [stmt (.createStatement conn)]
        (let [rs (sql-jdbc.execute/execute-statement! driver stmt "SELECT current_timezone() as \"time-zone\"")
              [{:keys [time-zone]}] (jdbc/result-set-seq rs)]
          time-zone)))))
