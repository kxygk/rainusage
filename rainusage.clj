(ns rainusage
  "Injesting Rainus data - and analysis"
  (:use [clojure.set])
  (:require [quickthing]
            [convertadoc]
            [tick.core       :as tick]
            [tech.v3.dataset :as ds ]
            [clojure.edn     :as edn ]))

(def collections
  (->> "/home/kxygk/Projects/rainusage/collections.edn"
       slurp
       edn/read-string))

(def equipment
  (->> "/home/kxygk/Projects/rainusage/equipment.edn"
       slurp
       edn/read-string))

(def locations
  (->> "/home/kxygk/Projects/rainusage/locations.edn"
       slurp
       edn/read-string))

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
  remove-invalid-logs
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
  injest-all-logs
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

(def gauge-logs
  (-> "/home/kxygk/Projects/rainusage/gauge"
      injest-all-logs
      remove-invalid-logs))

(defn
  sorted-dates?
  "Test is the dates are in chronological order
  If they are not, it's likely there was a typo"
  [collection-vec]
  (let [dates (->> collection-vec
                   (mapv :date))] 
    (->> dates
         (partition 2 1)
         (mapv (fn [[first-date
                     second-date]]
                 (assert (tick/> first-date
                                 second-date)
                         (str "\nDates out of order:\n"
                              "What should be the later date: "
                              first-date
                              "\nWhat should be the earlier date: "
                              second-date))))))
  true)
#_
(->> collections
     sorted-dates?)

(defn
  no-duplicate-locations?
  "Test is the dates are in chronological order
  If they are not, it's likely there was a typo"
  [collection-vec]
  (->> collection-vec
       (map (fn [one-day-collection]
              (->> one-day-collection
                   :samples
                   (mapv :location)
                   distinct?)))
       (every? identity)))
#_
(->> collections
     no-duplicate-locations?)

(defn
  collection-vec-to-map
  [collection-vec]
  (let [with-mapped-samples (->> collection-vec
                                 (mapv (fn [one-collection-day]
                                         (update one-collection-day
                                                 :samples
                                                 (fn [samples]
                                                   (zipmap (->> samples
                                                                (mapv :location))
                                                           #_samples
                                                           (->> samples
                                                                (mapv #(assoc %
                                                                              :date
                                                                              (:date one-collection-day))))))))))]
    
    (sorted-dates? with-mapped-samples) ;; throws error if not sorted
    (zipmap (->> with-mapped-samples
                 (mapv :date))
            (->> with-mapped-samples
                 #_(mapv (fn [collection-day]
                           (dissoc collection-day
                                   :date)))))))
#_
(-> collections
    collection-vec-to-map
    (clojure.pprint/pprint (clojure.java.io/writer "out/collection-map.edn")))


(defn-
  collections-before-date
  [date
   collections]
  (let [dates        (keys collections)
        dates-before (filter (partial tick/>
                                      date)
                             dates)]
    (select-keys collections
                 dates-before)))
#_
(-> collections
    collection-vec-to-map
    (collections-before-date #inst "2024-02-07")
    keys
    count)

(defn
  previous-collections
  "Find all the collections at `location`
  before `current-date`"
  [current-date
   location
   collections]
  (->> collections         ; take out collection
       (collections-before-date current-date) ; get the ones after the `current-date`
       (into (sorted-map)) ; sort them by time
       vals                ; get the collection info
       (mapv :samples)     ; get the samples
       (mapv location)     ; get the sample that's at our location (can be `nil`)
       (filterv some?)))   ; get the non-`nil` samples
#_
(->> collections
     collection-vec-to-map
     (previous-collections #inst "2024-02-07"
                           :ThMuCh0ConjoinedBottom)
     last)

(defn
  bottle-install-date
  "Given some `collections`, a `current-date` and a `location`..
  find the date the current bottle started collecting"
  [collections
   current-date
   location]
  (->> collections
       (previous-collections current-date
                             location)
       (filter #(-> %
                    :vial
                    some?))
       last
       :date))
#_
(bottle-install-date (->> collections
                          collection-vec-to-map)
                     #inst"2023-07-22"
                     :ThMuCh1LongLizard)
;; => #inst "2024-02-06T00:00:00.000-00:00";; => #inst "2023-01-09T00:00:00.000-00:00"
;; => #inst "2023-09-29T00:00:00.000-00:00"

(defn
  logger-install-date
  "Given some `collections`, a `current-date` and a `location`..
  find the date the current bottle was installed"
  [collections
   current-date
   location]
  (->> collections
       (previous-collections current-date
                             location)
       (filter #(-> %
                    :board
                    some?))
       last
       :date))
#_
(bottle-install-date (->> collections
                          collection-vec-to-map)
                     #inst "2024-02-07"
                     :ThMuCh2Sh01Thumb)
;; => #inst "2023-09-29T00:00:00.000-00:00"
;; => #inst "2023-01-09T00:00:00.000-00:00"

(-> locations
    :sepeleo
    keys)
;; => (:ThMuCh0Dip01
;;     :ThMuCh1Liz01
;;     :ThMuCh2Sh01Thumb
;;     :ThMuCh2Sh01B
;;     :ThMuCH2Sh02PairTall
;;     :ThMuCH2Sh02WhiteLoner
;;     :ThMuCH2SoloA)
#_
(-> gauge-logs
    ds/column-names)
;; => ("chipId"
;;     "timestampUTC"
;;     "unixtime"
;;     "secondstime"
;;     "tempC"
;;     "humidityPerc"
;;     "sourceFile")

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
  update-board-install-times
  "Go through the collection events and add a
  `install-time` to each,
  Based on when the last board take out at the location
  or a `:START` indicator"
  [collections]
  (update-vals collections
               (fn update-collection
                 [collection-day]
                 (update  collection-day
                          :samples
                          (fn update-samples
                            [samples]
                            (update-vals samples
                                         (fn update-sample
                                           [sample]
                                           (cond
                                             (-> sample ;; no board collected
                                                 :board
                                                 nil?)       sample
                                             (= :START  ;; no board collected by a new board was installed at the location
                                                (-> sample
                                                    :board)) sample
                                             :else           (let [install-date (logger-install-date collections
                                                                                                     (:date sample)
                                                                                                     (:location sample))]
                                                               (if (nil? install-date)
                                                                 (println (str "ERROR: "
                                                                               "Board collected with an unclear install time\n"
                                                                               "Location needs either:\n"
                                                                               "1. a prior board installed\n"
                                                                               "2. a `:START` indicator at a prior collection time\n"
                                                                               "Collection:"
                                                                               sample)))
                                                               (assoc sample
                                                                      :board-install-time
                                                                      install-date))))))))))
#_
(-> collections
    collection-vec-to-map
    update-board-install-times
    (clojure.pprint/pprint (clojure.java.io/writer "out/collection-map.edn")))

(defn
  update-chipids
  "Goes through the collection and adds `:chip-id`
  based on the board name"
  [collections]
  (update-vals collections
               (fn update-collection
                 [collection-day]
                 (update  collection-day
                          :samples
                          (fn update-samples
                            [samples]
                            (update-vals samples
                                         (fn update-sample
                                           [sample]
                                           (let [board-id (:board sample)
                                                 ]
                                             (if (or (nil? board-id)
                                                     (= :START
                                                        board-id))
                                               sample
                                               (let [chip-id (-> equipment
                                                                 :board-name2chip-id
                                                                 board-id)]
                                                 (if (nil? chip-id)
                                                   (do (println (str "ERROR: "
                                                                     "Specified `:board` "
                                                                     board-id
                                                                     " doesn't have a corresponding `chipid`"
                                                                     chip-id
                                                                     "\nCollection:"
                                                                     sample))
                                                       sample)
                                                   (assoc sample
                                                          :chip-id
                                                          chip-id))))))))))))
#_
(-> collections
    collection-vec-to-map
    update-board-install-times
    update-chipids
    (clojure.pprint/pprint (clojure.java.io/writer "out/collection-map.edn")))

(defn
  extract-gauge-log
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

(defn
  import-gauge-logs
  "Take a set of rain gauge `logs` and import them into a collection map.
  For this to work it needs to have had `:board-install-time` and `:chip-id` added"
  [collections
   logs]
  (update-vals collections
               (fn update-collection
                 [collection-day]
                 (update  collection-day
                          :samples
                          (fn update-samples
                            [samples]
                            (update-vals samples
                                         (fn update-sample
                                           [sample]
                                           (let [{:keys [chip-id
                                                         date
                                                         board-install-time]} sample]
                                             (if (and (some? chip-id)
                                                      (some? date)
                                                      (some? board-install-time))
                                               (assoc sample
                                                      :gauge-log
                                                      (extract-gauge-log logs
                                                                         board-install-time
                                                                         date
                                                                         chip-id))
                                               sample)))))))))
#_
(-> collections
    collection-vec-to-map
    update-board-install-times
    update-chipids
    (import-gauge-logs gauge-logs)
    (clojure.pprint/pprint (clojure.java.io/writer "out/collection-map.edn")))

(defn
  get-all-locations
  [collections]
  (apply union
         (->> collections
              vals
              (mapv :samples)
              (mapv keys)
              (mapv #(into #{} %)))))

(def locations
  (-> collections
      collection-vec-to-map
      update-board-install-times
      update-chipids
      get-all-locations))
;; => #{:ThMuCH2Sh02PairTall
;;      :ThMuOutsideWasp
;;      :ThMuCH2Sh02WhiteLoner
;;      :ThMuCh0ConjoinedBottom
;;      :ThMuCh2Sh01Thumb
;;      :ThMuOutsideDrone
;;      :ThMuCh2Sh01BrownTop
;;      :ThMuCh1LongLizard}

(assert (= 8
           (-> collections
               collection-vec-to-map
               update-board-install-times
               update-chipids
               get-all-locations
               count))
        (str "HI FUTURE GEORGE!! "
             "Code expects 8 locations! "
             "Either there was a typo"
             "or a new location was added"
             "and this assert needs to be updated haha ;)"))

(defn
  location-gauge-logs
  "Extract from `collections` the gauge logs for a `location`"
  [collections
   location]
  (ds/sort-by-column (->> collections
                          vals
                          (mapv :samples)
                          (mapv location)
                          (mapv :gauge-log)
                          (filterv some?)
                          flatten
                          (apply ds/concat))
                     "timestampUTC"))
#_
(-> collections
    collection-vec-to-map
    update-board-install-times
    update-chipids
    (import-gauge-logs gauge-logs)
    (location-gauge-logs :ThMuCH2Sh02PairTall))

(defn
  logs-by-location
  ([collections]
   (logs-by-location collections
                     (->> collections
                          get-all-locations)))
  ([collections
    locations]
   (->> locations
        (mapv (fn [location]
                (location-gauge-logs collections
                                     location)))
        (zipmap locations))))

(def location-logs
  (-> collections
      collection-vec-to-map
      update-board-install-times
      update-chipids
      (import-gauge-logs gauge-logs)
      logs-by-location))
#_
(-> location-logs
    :ThMuCH2Sh02PairTall)
#_
(keys
  location-logs)
;; => (:ThMuCH2Sh02PairTall
;;     :ThMuOutsideWasp
;;     :ThMuCH2Sh02WhiteLoner
;;     :ThMuCh0ConjoinedBottom
;;     :ThMuCh2Sh01Thumb
;;     :ThMuOutsideDrone
;;     :ThMuCh2Sh01BrownTop
;;     :ThMuCh1LongLizard)

(defn
  log2timediff
  "TODO add description
  rain gauge under normal conditions can't fill up faster than abbout 15 seconds"
  [gauge-logs
   & [{:keys [clicks-per-day-max
              clicks-per-day-min]
       :or   {unixtime-start nil
              clicks-per-day-max 1000000
              clicks-per-day-min 1}}]]
  (->> (-> gauge-logs
           (ds/column "unixtime"))
       (partition 2 1)
       (mapv (fn [[first-click
                   second-click]]
               [(/ second-click
                   (* 60.0 60.0 24.0))
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

(defn
  plot-location
  [location]
  (let [data (->> location-logs
                  location
                  log2timediff)]
    (if (empty? data)
      (println (str "Location `"
                    location
                    " doesn't have any data!"))
      (->> (-> (quickthing/primary-axis data
                                        {:x-name "Days since start"
                                         :y-name "Drips per day"
                                         :title  (str (symbol location))})
               (update :data
                       #(into %
                              (quickthing/dashed-line data)))
               (update :data
                       #(into %
                              (quickthing/adjustable-circles data
                                                             {:scale 5})))

               thi.ng.geom.viz.core/svg-plot2d-cartesian
               quickthing/svg-wrap
               quickthing/svg2xml)
           (spit (str "out/"
                      (symbol location)
                      ".svg"))))))
#_
(plot-location :ThMuCh1LongLizard)
#_
(->> location-logs
     keys
     (mapv plot-location))

(defn
  get-all-loggers
  [collections]
  (-> (->> collections
           vals
           (mapv :samples)
           (mapv vals)
           flatten
           (mapv :board)
           (into #{}))
      (disj :START)
      (disj nil)))
#_
(-> collections
    collection-vec-to-map
    get-all-loggers)

(defn
  logs-by-logger
  ([collections]
   (logs-by-logger collections
                     (->> collections
                          get-all-loggers)))
  ([collections
    loggers]
   (let [all-samples (->> collections
                          vals
                          (mapv :samples)
                          (mapv vals)
                          flatten)]
;   all-samples #_
   (->> loggers
        (mapv (fn [logger]
                (->> all-samples
                    (filterv #(= logger
                                 (:board %)))
                    (mapv :gauge-log)
                    (apply ds/concat))))
        (zipmap loggers)))))
#_
(-> collections
    collection-vec-to-map
    update-board-install-times
    update-chipids
    (import-gauge-logs gauge-logs)
    logs-by-logger)

(def logger-logs
  (-> collections
      collection-vec-to-map
      update-board-install-times
      update-chipids
      (import-gauge-logs gauge-logs)
      logs-by-logger))

(defn
  plot-all-loggers
  [by-logger]
  (->> by-logger
       (mapv (fn [[logger table]]
               (let [data (-> table
                              log2timediff)]
               (if (not-empty data)
                 (->> (-> (quickthing/primary-axis data
                                                   {:x-name "Days since start"
                                                    :y-name "Drips per day"
                                                    :title  (str (symbol logger))})
                          (update :data
                                  #(into %
                                         (quickthing/dashed-line data)))
                          (update :data
                                  #(into %
                                         (quickthing/adjustable-circles data
                                                                        {:scale 5})))
                          thi.ng.geom.viz.core/svg-plot2d-cartesian
                          quickthing/svg-wrap
                          quickthing/svg2xml)
                      (spit (str "out/"
                                 (symbol logger)
                                 ".svg")))))))))
#_
(-> logger-logs
    plot-all-loggers)

(let [locations (keys location-logs)
      loggers   (-> collections
                    collection-vec-to-map
                    get-all-loggers)]
  (spit "report.adoc"
        (str "= Autogenerated Logger Report"
             "\n\n"
             "== By Location"
             "\n\n"
             "Locations: \n\n"
             locations
             "\n\n"
             (apply str
                    (->> locations
                         (mapv (fn asciidoc-image-link-str
                                 [location]
                                 (str "image::"
                                      "out/"
                                      (symbol location)
                                      ".svg[]\n")))))
             "\n\n"
             "== By Logger"
             "\n\n"
             "Loggers: \n\n"
             loggers
             "\n\n"
             (apply str
                    (->> loggers
                         (mapv (fn asciidoc-image-link-str
                                 [logger]
                                 (str "image::"
                                      "out/"
                                      (symbol logger)
                                      ".svg[]\n")))))
             "\n\n")))

(convertadoc/to-html "report.adoc")
