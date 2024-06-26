(ns metabase.driver.implementation.ddl
    (:require
      [clojure.java.jdbc :as jdbc]
      [honeysql.core :as sql]
      [java-time :as t]
      [metabase.driver.ddl.interface :as ddl.i]
      [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
      [metabase.driver.sql.ddl :as sql.ddl]
      [metabase.public-settings :as public-settings]
      [metabase.query-processor :as qp]
      [metabase.util.i18n :refer [trs]])
    (:import com.mchange.v2.c3p0.C3P0ProxyConnection
      io.trino.jdbc.TrinoConnection
      [java.sql ResultSet Time Types Connection PreparedStatement ResultSetMetaData]
      java.sql.Time
      [java.time LocalTime OffsetDateTime ZonedDateTime LocalDateTime OffsetTime]
      java.time.format.DateTimeFormatter
      [java.time.temporal ChronoField Temporal]))

(set! *warn-on-reflection* true)

;(defn- set-statement-timeout!
;       "Must be called within a transaction.
;        Sets the current transaction `statement_timeout` to the minimum
;        of the current (non-zero) value and ten minutes.
;
;        This helps to address unexpectedly large/long running queries."
;       [tx]
;       (let [ten-minutes      (.toMillis (t/minutes 10))
;             new-timeout      (ten-minutes)]
;            ;; Can't use a prepared parameter with these statements
;            ;(sql.ddl/execute! tx [(format "SET LOCAL statement_timeout TO '%s'" (str new-timeout))])
;            ))

(defmethod ddl.i/refresh! :starburst [_driver database definition dataset-query]
           (let [{:keys [query params]} (qp/compile dataset-query)]
                (jdbc/with-db-connection [conn (sql-jdbc.conn/db->pooled-connection-spec database)]
                                         (println "===============starting refresh===============")
                                         (println "Setting client tags for conn (jdbc/get-connection conn)")
                                         (.setClientInfo (jdbc/get-connection conn) (doto (java.util.Properties.) (.putAll {"ClientTags" (str "-1, -999, analytics@curefit.com, not_available, non-priority, scheduled, default-hash")})))
                                         (.setAutoCommit (jdbc/get-connection conn) true)
                                         (jdbc/with-db-transaction [tx conn]
                                                                   (.setAutoCommit (jdbc/get-connection conn) true)
                                                                   (sql.ddl/execute! tx [(sql.ddl/drop-table-sql database (:table-name definition))])
                                                                   (.setAutoCommit (jdbc/get-connection conn) false))
                                         (jdbc/with-db-transaction [tx conn]
                                                                   (.setAutoCommit (jdbc/get-connection conn) true)
                                                                   (sql.ddl/execute! tx (into [(sql.ddl/create-table-sql database definition query)] params))
                                                                   (.setAutoCommit (jdbc/get-connection conn) false))
                                         {:state :success})))

(defmethod ddl.i/unpersist! :starburst
           [_driver database persisted-info]
           (jdbc/with-db-connection [conn (sql-jdbc.conn/db->pooled-connection-spec database)]
                                    (try
                                      (.setAutoCommit (jdbc/get-connection conn) true)
                                      (sql.ddl/execute! conn [(sql.ddl/drop-table-sql database (:table_name persisted-info))])
                                      (catch Exception e
                                        (throw e)))))

(defmethod ddl.i/check-can-persist :starburst
           [database]
           (let [schema-name (ddl.i/schema-name database (public-settings/site-uuid))
                 table-name  (format "persistence_check_%s" (rand-int 10000))
                 steps       [[:persist.check/create-schema
                               (fn check-schema [conn]
                                   (.setAutoCommit (jdbc/get-connection conn) true)
                                   (let [existing-schemas (->> ["show schemas"]
                                                               (sql.ddl/jdbc-query conn)
                                                               (map :schema)
                                                               (into #{}))]
                                        (or (contains? existing-schemas schema-name)
                                            (sql.ddl/execute! conn [(sql.ddl/create-schema-sql-data-lake database)]))))]
                              [:persist.check/create-table
                               (fn create-table [conn]
                                   (println "are we creating table?")
                                   (sql.ddl/execute! conn [(sql.ddl/create-table-sql database
                                                                                     {:table-name table-name
                                                                                      :field-definitions [{:field-name "field"
                                                                                                           :base-type :type/Text}]}
                                                                                     "select 1 as temp")]))]
                              [:persist.check/read-table
                               (fn read-table [conn]
                                   (println "are we reading table?")
                                   (sql.ddl/jdbc-query conn [(format "select * from %s.%s"
                                                                     schema-name table-name)]))]
                              [:persist.check/delete-table
                               (fn delete-table [conn]
                                   (sql.ddl/execute! conn [(sql.ddl/drop-table-sql database table-name)]))]
                              [:persist.check/create-kv-table
                               (fn create-kv-table [conn]
                                   (sql.ddl/execute! conn [(format "drop table if exists %s.cache_info"
                                                                   schema-name)])
                                   (.setAutoCommit (jdbc/get-connection conn) false)
                                   ;(sql.ddl/execute! conn (sql/format
                                   ;                         (ddl.i/create-kv-table-honey-sql-form schema-name)
                                   ;                         {:dialect :ansi}))
                                   )]
                              ;[:persist.check/populate-kv-table
                              ; (fn create-kv-table [conn]
                              ;     (sql.ddl/execute! conn (sql/format
                              ;                              (ddl.i/populate-kv-table-honey-sql-form
                              ;                                schema-name)
                              ;                              {:dialect :ansi})))]
                              ]]
                (jdbc/with-db-connection [conn (sql-jdbc.conn/db->pooled-connection-spec database)]
                                         (jdbc/with-db-transaction
                                           [tx conn]
                                           (loop [[[step stepfn] & remaining] steps]
                                                 (let [result (try (stepfn tx)
                                                                   (trs "Step {0} was successful for db {1}"
                                                                        step (:name database))
                                                                   ::valid
                                                                   (catch Exception e
                                                                     (println "Exception:" e)
                                                                     (trs "Error in `{0}` while checking for model persistence permissions." step)
                                                                     step))]
                                                      (cond (and (= result ::valid) remaining)
                                                            (recur remaining)

                                                            (= result ::valid)
                                                            [true :persist.check/valid]

                                                            :else [false step])))))))
