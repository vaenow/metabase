(ns metabase.query-processor-test.time-field-test
  (:require [metabase.query-processor-test :as qpt]
            [metabase.query-processor.middleware.expand :as ql]
            [metabase.test
             [data :as data]
             [util :as tu]]
            [metabase.test.data
             [dataset-definitions :as defs]
             [datasets :refer [*engine*]]]))

(qpt/expect-with-non-timeseries-dbs-except #{:oracle :mongo :redshift :presto}
  (if (= :sqlite *engine*)
    [[1 "Plato Yeshua" "2014-04-01 00:00:00" "08:30:00"]
     [4 "Simcha Yan" "2014-01-01 00:00:00" "08:30:00"]]

    [[1 "Plato Yeshua" "2014-04-01T00:00:00.000Z" "08:30:00.000Z"]
     [4 "Simcha Yan" "2014-01-01T00:00:00.000Z" "08:30:00.000Z"]])
  (->> (data/with-db (data/get-or-create-database! defs/test-data-with-time)
         (data/run-query users
                         (ql/filter (ql/between (ql/datetime-field $last_login_time :default )
                                                "08:00:00"
                                                "09:00:00"))
                         (ql/order-by (ql/asc $id))))
       qpt/rows))

(qpt/expect-with-non-timeseries-dbs-except #{:oracle :mongo :redshift :presto}
  (cond
    (= :sqlite *engine*)
    [[1 "Plato Yeshua" "2014-04-01 00:00:00" "08:30:00"]
     [4 "Simcha Yan" "2014-01-01 00:00:00" "08:30:00"]]

    (qpt/supports-report-timezone? *engine*)
    [[1 "Plato Yeshua" "2014-04-01T00:00:00.000-07:00" "00:30:00.000-08:00"]
     [4 "Simcha Yan" "2014-01-01T00:00:00.000-08:00" "00:30:00.000-08:00"]]

    :else
    [[1 "Plato Yeshua" "2014-04-01T00:00:00.000Z" "08:30:00.000Z"]
     [4 "Simcha Yan" "2014-01-01T00:00:00.000Z" "08:30:00.000Z"]])
  (tu/with-temporary-setting-values [report-timezone "America/Los_Angeles"]
    (->> (data/with-db (data/get-or-create-database! defs/test-data-with-time)
           (data/run-query users
             (ql/filter (ql/between (ql/datetime-field $last_login_time :default )
                                    "08:00:00"
                                    "09:00:00"))
             (ql/order-by (ql/asc $id))))
         qpt/rows)))
