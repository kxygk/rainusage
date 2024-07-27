(ns vial
  (:require [dk.ative.docjure.spreadsheet :as docjure]
            [tech.v3.dataset :as ds ]))

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
  (let [sheets    (->> filename
                       docjure/load-workbook
                       docjure/sheet-seq)
        vial-data (->> sheets
                       (mapv get-sheet-data))]
    (ds/filter-column (->> vial-data
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

(defn
  parse-excel-file
  [file-str]
  (let [sheets    (->> file-str
                       docjure/load-workbook
                       docjure/sheet-seq)
        vial-data (->> sheets
                       (mapv get-sheet-data))]
    (->> vial-data
         flatten
         (apply ds/concat))))
#_
(parse-excel-file "George (NTU).xlsx")

(defn get-data
  [vials-table
   vial-id]
(-> vials-table
    (ds/filter-column :vial-id
                      (-> vial-id
                          symbol
                          str))
    ds/rows))
#_
(-> "George (NTU).xlsx"
    parse-excel-file
    (get-data :ANQL))
;; => [{:vial-id "ANQL", :d18O -5.92389215670602, :d18O-sd 0.00499999999999989, :dD -36.6972828516432, :dD-sd 0.487906753386342}]
