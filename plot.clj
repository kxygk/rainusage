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
  axis-drip
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
                                                            [x (Math/log y)])))
                                               {:attribs {:stroke "none" #_"#0001"}})))
        (update :data
                #(into %
                       (quickthing/adjustable-circles (->> data
                                                           (mapv (fn [[x
                                                                       y
                                                                       radius
                                                                       attribs]]
                                                                   [x
                                                                    (Math/log y)
                                                                    radius ;; should be `nil` - ie. uses default
                                                                    attribs])))
                                                      {:scale 5
                                                       :attribs {:stroke "none" #_"#0002"
                                                                 :fill "#0002"}}))))))

(defn
  rain-gauges-data
  "Create a plot of rain gauge data"
  [logs
   plot-name
   [x-min
    x-max
    y-min
    y-max]]
  (-> (axis-drip plot-name
                 [x-min
                  x-max
                  y-min
                  y-max])
      (add-gauge-data logs)
      thi.ng.geom.viz.core/svg-plot2d-cartesian))

(defn-
  add-isotope-data
  "Add gauge data to a plot
  (or just a bare axis made by `plot/axis`)"
  [plot
   isotope-values]
  (let [data isotope-values]
    (-> plot
        (update :data
                #(into %
                       (quickthing/dashed-line data
                                               {:attribs {:fill   "#0088"
                                                          :stroke "#0084"}})))
        (update :data
                #(into %
                       (quickthing/adjustable-circles data
                                                      {:scale   10
                                                       :attribs {:fill   "#0088"
                                                                 :stroke "#0088"}}))))))

(defn
  axis-oxygen
  [[x-min
    x-max
    y-min
    y-max]]
  (let [dummy-data [[x-min y-min] ;; invisble data that sets the x/y ranges
                    [x-max y-max]]]
    (-> (quickthing/secondary-axis dummy-data
                                   {:x-name    (str "Years "
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
                                    :y-name    "d18O"
                                    #_#_:title plot-name #_ (str (symbol location))})
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

(defn
  isotopes
  [isotopes
   [x-min
    x-max
    y-min
    y-max]]
  (-> (axis-oxygen [x-min
                    x-max
                    y-min
                    y-max])
      (add-isotope-data isotopes)
      thi.ng.geom.viz.core/svg-plot2d-cartesian))

(defn
  write-all-locations
  "Given a map of locations to logs
  Plot each location in separate plots.
  Makes sure all the plots share a X and Y axis range"
  [logs-by-location
   isotope-by-location]
  (let [locations    (keys logs-by-location)
        [x-min
         x-max
         y-drip-min
         y-drip-max] (data-ranges logs-by-location)
        y-oxygen-min (->> isotope-by-location
                          vals
                          (apply concat)
                          (mapv second)
                          (apply min))
        y-oxygen-max (->> isotope-by-location
                          vals
                          (apply concat)
                          (mapv second)
                          (apply max))]
    (->> logs-by-location
         (map (fn print-each-logs-plot
                [[location-key
                  logs]]
                (if (-> logs
                        ds/row-count
                        zero?
                        not)
                  (->> (thi.ng.geom.svg.core/group {}
                                                   (plot/isotopes (location-key isotope-by-location)
                                                                  [x-min
                                                                   x-max
                                                                   y-oxygen-min
                                                                   y-oxygen-max])
                                                   (plot/rain-gauges-data logs
                                                                          (symbol location-key)
                                                                          [x-min
                                                                           x-max
                                                                           y-drip-min
                                                                           y-drip-max]))
                       quickthing/svg-wrap
                       quickthing/svg2xml
                       (spit (str "out/"
                                  (symbol location-key)
                                  ".svg"))))))
         doall)))
