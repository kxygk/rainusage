(ns collection
  (:require log
            [quickthing]
            [tock]
            [convertadoc]
            [tech.v3.dataset :as ds ]
            [tick.core       :as tick]))

#_
(def collections
  (->> "/home/kxygk/Projects/rainusage/collections.edn"
       slurp
       clojure.edn/read-string))
#_
(def equipment
  (->> "/home/kxygk/Projects/rainusage/equipment.edn"
       slurp
       clojure.edn/read-string))

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
  vec-to-map
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

(defn
  normalize-samples
  "return a long vector of all samples, with their dates embedded
  Note, this dumps the `:comment` tag"
  [collection-vec]
  (->> collection-vec
       (mapv (fn add-date
              [collection-event]
              (let [date (-> collection-event
                             :date)]
                (->> collection-event
                     :samples
                     (mapv (fn assoc-date-into-sample
                             [sample]
                             (-> sample
                                 (assoc :date
                                        date))))))))
       flatten
       (into [])))
#_
(->> rainusage/collections
     normalize-samples
     (group-by :date))

(defn-
  before-date
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
  previous
  "Find all the collections at `location`
  before `current-date`"
  [current-date
   location
   collections]
  (->> collections         ; take out collection
       (before-date current-date) ; get the ones after the `current-date`
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
       (previous current-date
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
       (previous current-date
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
  [equipment
   collections]
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
                                           (let [board-id (:board sample)]
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
(->> rainusage/collections
     collection/vec-to-map
     update-board-install-times
     (update-chipids equipment)
     (clojure.pprint/pprint (clojure.java.io/writer "out/collection-map.edn")))

(defn
  import-gauge-logs
  "Take a set of rain gauge `logs` and import them into a collection map.
  For this to work it needs to have had `:board-install-time` and `:chip-id` added"
  [logs
   collections]
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
                                                      (log/extract-gauge logs
                                                                         board-install-time
                                                                         date
                                                                         chip-id))
                                               sample)))))))))

(defn
  location-gauge-logs
  "Extract from `collections` the gauge logs for a `location`"
  [location
   collections]
  (ds/sort-by-column (->> collections
                          vals
                          (mapv :samples)
                          (mapv location)
                          (mapv :gauge-log)
                          (filterv some?)
                          flatten
                          (apply ds/concat))
                     "timestampUTC"))

(defn
  by-location
  [locations
   collections]
  (->> locations
       (mapv (fn [location]
               (location-gauge-logs location
                                    collections)))
       (zipmap locations)))


(defn
  import-vials
  [vials
   collections]
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
                                           (let [{:keys [vial]} sample]
                                             (if (some? vial)
                                               (do (println (str "Vial: " vial))
                                               (assoc sample
                                                      :vial-data
                                                      (vial/get-data vials
                                                                     vial))))))))))))
#_
(clojure.pprint/pprint (->> rainusage/collections
                            vec-to-map
                            update-board-install-times
                            (update-chipids rainusage/equipment)
                            ;;(import-gauge-logs rainusage/gauge-logs)
                            (import-vials (vial/parse-excel-file "George (NTU).xlsx")))
                       (clojure.java.io/writer "out/with-vial-data-map.edn"))


#_
(vial/get-data (vial/parse-excel-file "George (NTU).xlsx")
               :AGDX)
