(ns plot
  (:require [log]
            [quickthing]
            [tock]
            [convertadoc]
            [tech.v3.dataset :as ds ]
            [tick.core :as tick]))

(defn- data-ranges
  "Creates a dummy data vector to set `x/y min/max` bounds.
  Input: a map of logger names to datatables.
  These get flattened and a x min/max is found from all aggrigated logs"
  [loggernames2logs
   & [{:keys [y-min  ;; these are guesstimated..
              y-max] ;; but I guess you could also calculate
       :or   {y-min 0
              y-max 13}}]]
  (let [all-time-stamps (->> loggernames2logs
                             vals
                             (mapv #(ds/column %
                                               "unixtime"))
                             flatten
                             (into []))]
    [(apply min all-time-stamps) ; x-min
     (apply max all-time-stamps) ; x-max
     0                           ; y-min
     13]))                        ; y-max


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

(defn
  rain-gauges-data
  "Create a plot of"
  [logs
   plot-name
   [x-min
    x-max
    y-min
    y-max]]
  (let [dummy-data [[x-min y-min] ;; invisble data that sets the x/y ranges
                    [x-max y-max]]
        data       (->> logs
                  log/log2timediff)]
    (if (empty? data)
      (println "given logs are empty!"))
    (-> (quickthing/primary-axis dummy-data
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
                                  :title  plot-name #_ (str (symbol location))})
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
        quickthing/svg2xml)))


(defn
  write-all-locations
  "Given a map of locations to logs
  Plot each location in separate plots.
  Makes sure all the plots share a X and Y axis range"
  ([logs-by-location]
   (write-all-locations logs-by-location
                        (data-ranges logs-by-location)))
  ([logs-by-location
    data-ranges]
   (->> logs-by-location
        (map (fn print-each-logs-plot
                 [[location-key
                   logs]]
               (if (-> logs
                       ds/row-count
                       zero?
                       not)
                 (->> (rain-gauges-data logs
                                        (symbol location-key)
                                        data-ranges)
                      (spit (str "out/"
                                 (symbol location-key)
                                 ".svg"))))))
        doall)))


#_
(defn
  plot-all-loggers
  "All loggers are plotted at once,
  b/c their global start and end times set the `x` bounds"
  [by-logger]
  (let [dummy-data (gen-dummy-data by-logger)]
    (->> by-logger
         (mapv (fn [[logger
                     table]]
                 (let [data (-> table
                                log/log2timediff)]
                   (if (not-empty data)
                     (->> (-> (quickthing/primary-axis dummy-data
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
                                     ".svg"))))))))))

#_
(-> dummy-data
    (quickthing/primary-axis {:x-name "Days since start"
                              :y-name "Drips per day"
                              :title  "Test"}))
