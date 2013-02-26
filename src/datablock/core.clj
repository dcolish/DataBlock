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


(def long-op-deserializer LongOp/DESERIALIZER)
(def read-combine-cas DbHarness$CommitType/READ_COMBINE_CAS)
(def hour-month-day-hours HourDayMonthBucketer/hours)


(defn mapDbHarness [backing-map deserializer commit-type id-service]
  (MapDbHarness. backing-map deserializer commit-type id-service))


(defn dimension [name bucketer id-sub field-bytes]
  (Dimension. name bucketer id-sub field-bytes))


(defn rollup [dimension bucket-type] (Rollup. dimension bucket-type))


(defn newCube [dimensions rollups] 
  (DataCube. dimensions rollups))


(defn newCubeIo [cube db batch-size millis sync-level]
  (DataCubeIo. cube db batch-size millis sync-level))


(defn -main
  [& args]
  
  (let [dbHarness (mapDbHarness (ConcurrentHashMap.) long-op-deserializer read-combine-cas nil)
        hour-month-day-bucketer (HourDayMonthBucketer.)
        time (dimension "time" hour-month-day-bucketer false 8)
        hourRollup (rollup time hour-month-day-hours)
        cube (newCube [time] [hourRollup])
        cubeIo (newCubeIo cube dbHarness 1 Long/MAX_VALUE SyncLevel/FULL_SYNC)]

    (.writeSync cubeIo (LongOp. 10) (-> (WriteBuilder. cube) (.at time (now))))

    (let [thing (.get cubeIo (-> (ReadBuilder. cube) (.at time hour-month-day-hours (now))))]
      (when (.isPresent thing)
        (println (-> (.get thing) .getLong))))))
