(ns datablock.core
  (:use [clj-time.core :only (now)] 
        [clojure.tools.logging])
  (:import [java.util.concurrent ConcurrentHashMap]
   
           [org.joda.time DateTime DateTimeZone]

           [com.urbanairship.datacube DataCube DataCubeIo DbHarness$CommitType DbHarness Dimension 
                                      Op ReadBuilder Rollup SyncLevel WriteBuilder]
           [com.urbanairship.datacube.bucketers HourDayMonthBucketer StringToBytesBucketer]
           [com.urbanairship.datacube.dbharnesses MapDbHarness]
           [com.urbanairship.datacube.ops LongOp]))


(def read-combine-cas DbHarness$CommitType/READ_COMBINE_CAS)
(def hour-month-day-hours HourDayMonthBucketer/hours)


(defmacro io-builder [klass cube method & ats]
  (list* 'doto `(new ~klass ~cube)
    (for [args (filter #(not (empty? %)) ats)]
      `(~method ~@args))))


(defn write-io [io value builder]
  (.writeSync io (LongOp. value) builder))


(defn get-io [io builder]
  (let [value (.get io builder)]
      (when (.isPresent value)
        (println (-> (.get value) .getLong))
        )
      ))


(defn -main
  [& args]

  (let [dbHarness (MapDbHarness. (ConcurrentHashMap.) LongOp/DESERIALIZER read-combine-cas nil)
        hour-month-day-bucketer (HourDayMonthBucketer.)
        dim-time (Dimension. "time" hour-month-day-bucketer false 8)
        hourRollup (Rollup. dim-time hour-month-day-hours)
        cube (DataCube. [dim-time] [hourRollup])
        cubeIo (DataCubeIo. cube dbHarness 1 Long/MAX_VALUE SyncLevel/FULL_SYNC)]

    (write-io cubeIo 10 (io-builder WriteBuilder cube .at [dim-time (now)]))
    (get-io cubeIo (io-builder ReadBuilder cube .at [dim-time hour-month-day-hours (now)]))
    )
  )
