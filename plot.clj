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

(defn-
  axis
  [plot-name
   [x-min
    x-max
    y-min
    y-max]]
  (let [dummy-data [[x-min y-min] ;; invisble data that sets the x/y ranges
                    [x-max y-max]]]
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
            (assoc-in [:x-axis
                       :major]
                      (tock/month-start-unix-times (-> dummy-data
                                                       first
                                                       first
                                                       tock/unix-time-sec2date)
                                                   (-> dummy-data
                                                       second
                                                       first
                                                       tock/unix-time-sec2date))))))

(defn-
  add-gauge-data
  "Add gauge data to a plot
  (or just a bare axis made by `plot/axis`)"
  [plot
   logs]
  (let [data (->> logs
                  log/log2timediff)]
    (-> plot
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
                                                      {:scale 5}))))))

(defn
  rain-gauges-data
  "Create a plot of rain gauge data"
  [logs
   plot-name
   [x-min
    x-max
    y-min
    y-max]]
  (-> (axis plot-name
            [x-min
             x-max
             y-min
             y-max])
      (add-gauge-data logs)
      thi.ng.geom.viz.core/svg-plot2d-cartesian
      quickthing/svg-wrap
      quickthing/svg2xml))

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
