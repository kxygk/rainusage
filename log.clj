(ns log
  "Utilities for parsing Rainus logs"
  (:require [quickthing]
            [tock]
            [convertadoc]
            [tick.core       :as tick]
            [tech.v3.dataset :as ds ]))

(defn-
  update-v1-log
  "Take the Rainus V1 4 column format and convert to a 6 column V2 log
  The Temp/Hum columns are padded with invalid values `-777.0`
  NOTE:
  The default values for when there is no temp/hum reading is `-666.0`
  This allows updated logs to be distinguished
  from ones that had a disconnected sensor"
  [v1-log]
  (-> v1-log
      (ds/add-or-update-column "tempC"
                               -777.0) ;;`tempC` col
      (ds/add-or-update-column "humidityPerc"
                               -777.0))) ;;`humidityPerc` col

(defn
  remove-invalid
  "Goes through the logs and removes invalid logs.
  Spits them to `./out/invalid-logs.txt` file"
  [logs]
  (let [validated-logs (-> logs
                           (ds/group-by (fn invalid-timestamp
                                          [log]
                                          (try
                                            (-> log
                                                (get "timestampUTC")
                                                tick/date-time
                                                tick/instant)
                                            :valid
                                            (catch Exception e
                                              :invalid)))))]
    (->  validated-logs
         :invalid
         (ds/write! "out/invalid-logs.csv"))
    (-> validated-logs
        :valid)))
#_
(-> gauge-logs
    remove-invalid-logs)

(defn
  read-rainus-log
  "Reads a Rainus log file into a tech/ml/`dataset`"
  [log-filestr]
  (let [raw-log (-> log-filestr
                    (ds/->dataset {:file-type   :csv
                                   :header-row? false}))]
    (-> (if (== 4.0
                (ds/column-count raw-log))
          (update-v1-log raw-log)
          raw-log)
        (ds/rename-columns ["chipId"
                            "timestampUTC"
                            "unixtime"
                            "secondstime"
                            "tempC"
                            "humidityPerc" ])
        (ds/add-or-update-column "sourceFile"
                                 log-filestr))))

(defn
  injest-from-dir
  "Goes into the directory `log-dirstr`
  and reads in all the files as logs
  NOTE:
  Rainus V1 logs will be updated to be V2 compatible"
  [log-dirstr]
  (let [filepaths (->> log-dirstr
                       java.io.File.
                       .list
                       sort
                       (mapv (partial str
                                      log-dirstr
                                      "/")))
        all-logs  (->> filepaths
                       (mapv read-rainus-log)
                       (apply ds/concat))]
    (-> all-logs
        (ds/sort-by-column "timestampUTC") ;; sort by time
        (ds/unique-by #(dissoc % ; remove redundant logs (should be a lot!)
                               "sourceFile"))))) ;; can be across different files
#_
(-> "/home/kxygk/Projects/rainusage/gauge"
    injest-all-logs
    (ds/write! "out/all-logs.csv"))


(defn
  get-logs
  "Filter logs to get logs that match a `chipId`
  and a `start-time` `end-time` time range"
  [logs
   start-time
   end-time
   chip-id]
  (-> logs
      (ds/filter-column "chipId"
                        (partial =
                                 chip-id))
      (ds/filter-column "timestampUTC"
                        (fn [time-stamp]
                          (let [tick-time (tick/instant (tick/date-time time-stamp))]
                            (and (tick/> tick-time
                                         start-time)
                                 (tick/< tick-time
                                         end-time)))))))
#_
(-> gauge-logs
    (get-logs #inst"2022-11-27"
              #inst"2022-11-29"
              8373316))

(defn
  extract-gauge
  [gauge-log
   start-time
   end-time
   chip-id]
  (-> gauge-log
      (ds/filter-column "chipId"
                        (partial =
                                 chip-id))
      (ds/filter-column "timestampUTC"
                        #(let [time-inst (clojure.instant/read-instant-date %) #_ (-> %
                                                                                      tick/date-time
                                                                                      tick/instant)]
                           (cond
                             (tick/< time-inst
                                     start-time) false
                             (tick/> time-inst
                                     end-time)   false
                             :else               true)))))
#_
(extract-gauge-log gauge-logs
                   #inst"2022-11-27"
                   #inst"2022-11-29"
                   8373316)
#_
(def location-logs
  (-> collections
      collection-vec-to-map
      update-board-install-times
      update-chipids
      (import-gauge-logs gauge-logs)
      logs-by-location))

(defn
  log2timediff
  "Goes into a table of logs
  (which is arranged chronologically)
  and calculates the time difference between adjacent rows.
  So you get a N-1 vector out if all data is normal
  NOTE:
  There are additional optional args to filter misclicks,
  or clicks made during testing.
  Rain gauges normally won't click more than one every 15 seconds.
  So the defaults are set to filter any faster click"
  [gauge-logs
   & [{:keys [clicks-per-day-max
              clicks-per-day-min]
       :or   {unixtime-start     nil
              clicks-per-day-max 1000000
              clicks-per-day-min 1}}]]
  (->> (-> gauge-logs
           (ds/column "unixtime"))
       (partition 2 1)
       (mapv (fn [[first-click
                   second-click]]
               [second-click
                (/ (* 60.0 60.0 24.0)
                   (- second-click
                      first-click))]))
       (filterv (fn [[time
                      clicks-per-day]]
                  (and (> clicks-per-day
                          clicks-per-day-min)
                       (< clicks-per-day
                          clicks-per-day-max))))))
#_
(->> location-logs
     :ThMuCh1LongLizard
     log2timediff)
