(ns rainusage
  "Injesting Rainus data - and analysis"
  (:use [clojure.set])
  (:require [log]
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
  "TODO add description
  rain gauge under normal conditions can't fill up faster than abbout 15 seconds"
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

(def
  date-formatter
  "A `geom` label formatter to give human dates on the X axis
  Expects the X axis to be in `unix-time` (in seconds)"
  (thi.ng.geom.viz.core/default-svg-label (fn [unix-time]
                                            (println (str "unix-time: "
                                                          unix-time))
                                            (->> unix-time
                                                 tock/unix-time-sec2date
                                                 (tick/format (tick/formatter "MMM")))))) ;;"dMMM''yy"
#_
(date-formatter [10 10]  1000)
;; => [:text {:x "10.00", :y "10.00"} "Jan"]

(def dummy-data
  (let [all-time-stamps (->> location-logs
                             vals
                             (mapv #(ds/column %
                                               "unixtime"))
                             flatten
                             (into []))]
    (let [min-x (apply min all-time-stamps)
          max-x (apply max all-time-stamps)]
      [[min-x 0]
       [max-x 13]])))

(defn
  plot-location
  "TODO: `dummy-data` and `location-logs` are taken from the global namespace :S"
  [location]
  (let [data (->> location-logs
                  location
                  log2timediff)]
    (if (empty? data)
      (println (str "Location `"
                    location
                    " doesn't have any data!"))
      (->> (-> (quickthing/primary-axis dummy-data #_(into dummy-data data)
                                        {:x-name (str "Years "
                                                      (->> dummy-data
                                                           first
                                                           first
                                                           tock/unix-time-sec2date
                                                           (tick/format (tick/formatter "yyyy")))
                                                      " to "
                                                      (->> dummy-data
                                                           second
                                                           first
                                                           tock/unix-time-sec2date
                                                           (tick/format (tick/formatter "yyyy"))))
                                         :y-name "log(Drips per day)"
                                         :title  (str (symbol location))})
               (assoc-in [:x-axis :label]
                         date-formatter)
               (update :data
                       #(into %
                              (quickthing/dashed-line (->> data
                                                          (mapv (fn [[x
                                                                      y]]
                                                                  [x (Math/log y)]))))))
               (update :data
                       #(into %
                              (quickthing/adjustable-circles (->> data
                                                                 (mapv (fn [[x
                                                                             y]]
                                                                         [x (Math/log y)])))
                                                             {:scale 5})))
               (assoc-in [:x-axis
                          :major]
                         (tock/month-start-unix-times (-> dummy-data
                                                          first
                                                          first
                                                          tock/unix-time-sec2date)
                                                      (-> dummy-data
                                                          second
                                                          first
                                                          tock/unix-time-sec2date)))
               thi.ng.geom.viz.core/svg-plot2d-cartesian
               quickthing/svg-wrap
               quickthing/svg2xml)
           (spit (str "out/"
                      (symbol location)
                      ".svg"))))))
#_
(-> dummy-data
    (quickthing/primary-axis {:x-name "Days since start"
                              :y-name "Drips per day"
                              :title  "Test"}))

;; !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! GENERATES ALL LOCATION PLOTS
(->> location-logs
     keys
     (mapv plot-location)) ;; doesn't work..

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

(defn
  plot-all-loggers
  "TODO: `dummy-data` taken from the global namespace :S"
  [by-logger]
  (->> by-logger
       (mapv (fn [[logger
                   table]]
               (let [data (-> table
                              log2timediff)]
                 (if (not-empty data)
                   (->> (-> (quickthing/primary-axis dummy-data #_(into dummy-data data)
                                                     {:x-name (str "Years "
                                                                   (->> dummy-data
                                                                        first
                                                                        first
                                                                        tock/unix-time-sec2date
                                                                        (tick/format (tick/formatter "yyyy")))
                                                                   " to "
                                                                   (->> dummy-data
                                                                        second
                                                                        first
                                                                        tock/unix-time-sec2date
                                                                        (tick/format (tick/formatter "yyyy"))))
                                                      :y-name "log(Drips per day)"
                                                      :title  (-> logger
                                                                  symbol
                                                                  str)})
                            (assoc-in [:x-axis :label]
                                      date-formatter)
                            (update :data
                                    #(into %
                                           (quickthing/dashed-line (->> data
                                                                        (mapv (fn [[x
                                                                                    y]]
                                                                                [x (Math/log y)]))))))
                            (update :data
                                    #(into %
                                           (quickthing/adjustable-circles (->> data
                                                                               (mapv (fn [[x
                                                                                           y]]
                                                                                       [x (Math/log y)])))
                                                                          {:scale 5})))
                            (assoc-in [:x-axis
                                       :major]
                                      (tock/month-start-unix-times (-> dummy-data
                                                                       first
                                                                       first
                                                                       tock/unix-time-sec2date)
                                                                   (-> dummy-data
                                                                       second
                                                                       first
                                                                       tock/unix-time-sec2date)))
                            thi.ng.geom.viz.core/svg-plot2d-cartesian
                            quickthing/svg-wrap
                            quickthing/svg2xml)
                        (spit (str "out/"
                                   (symbol logger)
                                   ".svg")))))))))

;; !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! GENERATES ALL LOCATION PLOTS
(-> logger-logs
    plot-all-loggers)

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

(defn
  get-sheet-errors
  "Get errors from one sheet's analysis
  The values at the top...
  TODO: Ask Harsh about them"
  [sheet]
  (->> sheet
       (docjure/select-columns {:D :external-precision}) ;; unused keys
       (take 2)
       (map vals)
       flatten
       (zipmap [:d18O
                :dD])))
#_
(let [filename "George (NTU).xlsx"]
  (let [sheets (->> filename
                    docjure/load-workbook
                    docjure/sheet-seq)]
    (->> sheets
         (map get-sheet-errors))))

(defn
  get-sheet-data
  "extract a sheet's data section"
  [sheet]
  (->> sheet
       (docjure/select-columns {:A :vial-id
                                :B :d18O
	                        :C :d18O-sd
	                        :D :dD
	                        :E :dD-sd})
       (drop 3)
       ds/->dataset))
#_
(let [filename "George (NTU).xlsx"]
  (let [sheets (->> filename
                    docjure/load-workbook
                    docjure/sheet-seq)]
    (->> sheets
         first
         get-sheet-data)))
#_
(let [filename "George (NTU).xlsx"]
  (let [sheets (->> filename
                    docjure/load-workbook
                    docjure/sheet-seq)]
    (-> sheets
        first
        get-sheet-data
        (ds/filter-column :vial-id (partial =
                                            "ANQL")))))

(defn
  vial-seq-2-map
  "Turns the sequence of vials read in using `docjure`
  into a mapping from `vial-id` to vial data
  TODO: Unclear we really should use this..
  The vial may have been reanlysed.
  Then you'd have multiple rows with the same vial ID.
  This example should be handled explicitely.
  Right now I assume it just won't happen.."
  [seq-of-vials]
  (let [vial-map (zipmap (->> seq-of-vials
                              (map :vial-id)
                              (map keyword))
                         (->> seq-of-vials
                              (map #(dissoc %
                                            :vial-id))))]
    (if (= (count seq-of-vials)
           (count vial-map))
      vial-map
      (throw (Exception."Redundant Vial ID!")))))
#_
(let [filename "George (NTU).xlsx"]
  (let [sheets (->> filename
                    docjure/load-workbook
                    docjure/sheet-seq)]
    (->> sheets
         first
         get-sheet-data
         vial-seq-2-map)))


#_
(let [filename "George (NTU).xlsx"]
  (let [sheets (->> filename
                    docjure/load-workbook
                    docjure/sheet-seq)
        vial-data (->> sheets
                       (mapv get-sheet-data))]
    (ds/filter-column (->> vial-date
                           flatten
                           (apply ds/concat))
                      :vial-id
                      (partial =
                               "ANQL"))))
;; => _unnamed [260 5]:
;;    | :vial-id |   :d18O | :d18O-sd |    :dD |   :dD-sd |
;;    |----------|---------|----------|--------|----------|
;;    |     ANQL |  -5.924 | 0.005000 | -36.70 |   0.4879 |
;;    |     ANCF |  -5.885 |  0.01947 | -35.16 |  0.06178 |
;;    |     AZUQ |  -5.884 | 0.006658 | -34.84 |   0.1813 |
;;    |     ATHN |  -5.872 | 0.007767 | -34.35 |   0.1372 |
;;    |     ABBR |  -5.850 |  0.01553 | -34.15 |  0.07839 |
;;    |     ASUW |  -5.403 |  0.02128 | -32.46 |  0.03156 |
;;    |     AYHA |  -5.685 |  0.02000 | -32.91 | 0.009000 |
;;    |     AZRC |  -5.692 |  0.03853 | -32.97 |  0.02816 |
;;    |     ADYZ |  -5.580 |  0.01054 | -32.69 |  0.09920 |
;;    |     AZAK |  -5.679 |  0.03001 | -32.88 |  0.04484 |
;;    |      ... |     ... |      ... |    ... |      ... |
;;    |     ACLN |  0.9847 |  0.04500 |  3.349 |   0.3764 |
;;    |     AGWT | -0.3302 |  0.02868 | -11.11 |   0.1124 |
;;    |     AWZS |  -1.839 |  0.09151 | -17.55 |   0.3841 |
;;    |     ADWW |  0.4479 |  0.04158 | -8.148 |   0.5469 |
;;    |     AVYG | -0.5079 |  0.02079 | -11.95 |   0.3783 |
;;    |     ARFE |   1.335 |  0.01328 | -4.989 |   0.2222 |
;;    |     ARBS | -0.6111 |  0.05201 | -10.58 |   0.2222 |
;;    |     ANHW |   1.420 |  0.06110 | -6.243 |   0.4589 |
;;    |     AZXQ | -0.7761 |  0.05150 | -14.75 |   0.2894 |
;;    |     ACCW |  0.8718 |  0.05991 | -8.277 |  0.01601 |
;;    |     AGDX |   1.734 | 0.009644 | -1.663 |   0.1014 |

#_
(let [filename "George (NTU).xlsx"]
  (let [sheets (->> filename
                    docjure/load-workbook
                    docjure/sheet-seq)]
    (->> sheets
         (map (fn [sheet]
                (->> sheet
                     (docjure/select-columns {:D :external-precision})
                     (take 2)
                     (map vals)
                     flatten))))))

