(ns
    rainusage
  "Utility library for data produced by Rainus loggers"
  (:require clojure.data.csv
            [tick.core :as tick]))

(defn-
  csv-2-map
  "Takes a CSV (as coming from `clojure.data.csv`)
  Turn it into Vec of Maps
  With Map keys being the column names
  Key names are cleaned to not have spaces.
  Spaces are replaces with `-`
  Data line that look incomplete are removed
  ie. those where the first or last column is empty
  This allows users to have pseudo comments in-file"
  [csv]
  (let [header [:id ;; unique ID strings on each board
                :time ;; time in iso8601 format
                :unix ;; secs since 1970-01-01 00:00:00
                :millenium] ;; secs since 2000-01-01 00:00:00
        body (->> 
               (rest
                 csv)
               ;; remove line if looks like no data
               (filterv
                 #(and
                     (-> ;; if first entry is empty ""
                       %
                       first
                       empty?
                       not)
                     (-> ;; if last entry is empty ""
                       %
                       last
                       empty?
                       not))))]
               (->>
      body
      (mapv
        #(zipmap
           header
           %)))))

(defn
  read-file
  "Read a Rainus log file into a vector of maps.
  keys are from the headers (hardcoded into the Rainus)
  `:id` - unique ID strings on each board
  `:time` - time in iso8601 format - read into a `tick/data-time`
  `:unix` - secs since 1970-01-01 00:00:00
  `:millenium` - secs since 2000-01-01 00:00:00"
  [file]
  (->>
    file
    slurp
    clojure.data.csv/read-csv
    csv-2-map
    (mapv
      (fn [log]
        (->
          log
          (update
            :id
            #(Integer/parseInt
               %))
          (update
            :time
            tick/date-time)
          (update
            :unix
            #(Integer/parseInt
               %))
          (update
            :millenium
            #(Integer/parseInt
               %)))))))
#_
(read-file
  "00rainLog.txt")
;; => [{:id 8371400,
;;      :time #time/date-time "2022-12-29T20:25:22",
;;      :unix 1672345522,
;;      :millenium 725660722}
;;     {:id 8371400,
;;      :time #time/date-time "2022-12-29T21:43:18",
;;      :unix 1672350198,
;;      :millenium 725665398}
;;...
;;...
;;...
;;     {:id 8371400,
;;      :time #time/date-time "2023-01-08T08:31:04",
;;      :unix 1673166664,
;;      :millenium 726481864}]

(defn
  update-ids
  "Replace `rainusid`s
  (ie. hardcoded IDs in the microcontroller boards)
  with
  `location` provided by the user in a mapping
  (ie. ~human readable~ names)"
  [rainusids-2-locations
   logs]
  (->>
    logs
    (mapv
      (fn
        [log]
        (update
          log
          :id
          #(get
             rainusids-2-locations
             %))))))
#_
(->>
  (read-file
    "00rainLog.txt")
  (update-ids
    {8371400 "butts"
     8371401 "tits"}))




#_
(let [input-dir  "/home/kxygk/Projects/stars/data/gauge/"]
  (let [files (->>
                input-dir
                java.io.File.
                .list
                sort)]
    (let [file-paths (->>
                       files
                       (map
                         #(str
                            input-dir
                            %)))]
      (->>
        file-paths
        (mapcat
          read-file)
        (group-by
          :id)
        keys))))
