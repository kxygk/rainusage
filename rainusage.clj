(ns rainusage
  "Injesting Rainus data - and analysis"
  (:use [clojure.set])
  (:require [log]
            [plot]
            [collection]
            [quickthing]
            [tock]
            [convertadoc]
            [tick.core       :as tick]
            [tech.v3.dataset :as ds ]
            [clojure.java.io :as io]
            [dk.ative.docjure.spreadsheet :as docjure]
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

(def gauge-logs
  (-> "/home/kxygk/Projects/rainusage/gauge"
      log/injest-from-dir
      log/remove-invalid))


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
  get-all-locations
  [collections]
  (apply union
         (->> collections
              vals
              (mapv :samples)
              (mapv keys)
              (mapv #(into #{} %)))))

(def locations
  (->> collections
       collection/vec-to-map
       collection/update-board-install-times
       (collection/update-chipids equipment)
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
           (->> collections
                collection/vec-to-map
                collection/update-board-install-times
                (collection/update-chipids equipment)
                get-all-locations
                count))
        (str "HI FUTURE GEORGE!! "
             "Code expects 8 locations! "
             "Either there was a typo"
             "or a new location was added"
             "and this assert needs to be updated haha ;)"))


#_
(->> collections
     collection/vec-to-map
     collection/update-board-install-times
     (collection/update-chipids equipment)
     (collection/import-gauge-logs gauge-logs)
     (collection/location-gauge-logs :ThMuCH2Sh02PairTall))

(def location-logs
  (->> collections
       collection/vec-to-map
       collection/update-board-install-times
       (collection/update-chipids equipment)
       (collection/import-gauge-logs gauge-logs)
       (collection/by-location locations)))

(spit "out/logs-by-location.edn"
      location-logs)
#_
(-> collections
    collection-vec-to-map
    update-board-install-times
    update-chipids
    (import-gauge-logs gauge-logs))

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


;; !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! GENERATES ALL LOCATION PLOTS

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
    collection/vec-to-map
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
     (->> loggers
          (mapv (fn [logger]
                  (->> all-samples
                       (filterv #(= logger
                                    (:board %)))
                       (mapv :gauge-log)
                       (apply ds/concat))))
          (zipmap loggers)))))
#_
(->> collections
     collection/vec-to-map
     collection/update-board-install-times
     (collection/update-chipids equipment)
     (collection/import-gauge-logs gauge-logs)
     logs-by-logger
     :R10)

(def logger-logs
  (->> collections
       collection/vec-to-map
       collection/update-board-install-times
       (collection/update-chipids equipment)
       (collection/import-gauge-logs gauge-logs)
       logs-by-logger))

;; !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! GENERATES ALL LOCATION PLOTS

(-> logger-logs
    plot/write-all-locations)

(-> location-logs
    plot/write-all-locations)

(let [locations (keys location-logs)
      loggers   (-> collections
                    collection/vec-to-map
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


(def time-18o-pairs
  (->> collections
       collection/normalize-samples
       (collection/import-vials (vial/parse-excel-file "George (NTU).xlsx"))
       (filterv #(= :ThMuCh2Sh01BrownTop
                    (-> %
                        :location)))
       collection/time-vs-18O))
#_
(-> time-18o-pairs)


(->> locations
     (mapv (fn plot-location-isotope
             [location]
             (let [time-18o-pairs (->> collections
                                       collection/normalize-samples
                                       (collection/import-vials (vial/parse-excel-file "George (NTU).xlsx"))
                                       (filterv #(= location
                                                    (-> %
                                                        :location)))
                                       collection/time-vs-18O)]
               (if (-> time-18o-pairs
                       empty?
                       not)
                 (->> (plot/isotopes time-18o-pairs
                                     [(->> time-18o-pairs
                                           (mapv first)
                                           (apply min))
                                      (->> time-18o-pairs
                                           (mapv first)
                                           (apply max))
                                      (->> time-18o-pairs
                                           (mapv second)
                                           (apply min))
                                      (->> time-18o-pairs
                                           (mapv second)
                                           (apply max))])
                      (spit (str "out/"
                                 (symbol location)
                                 ".svg"))))))))



(let [o18-by-location (update-vals (->> collections
                                        collection/normalize-samples
                                        (collection/import-vials (vial/parse-excel-file "George (NTU).xlsx"))
                                        (group-by :location))
                                   collection/time-vs-18O)]
  (plot/write-all-locations location-logs
                            o18-by-location))
