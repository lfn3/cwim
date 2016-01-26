(ns cwim.core-test
  (:require [clojure.test :refer :all]
            [cwim.core :refer :all]))

(deftest add-node-test
  (testing "Add a single node"
    (let [srv-map {:state (atom {:cwim.core/nodes '()})}
          updated-map (add-node srv-map "0.0.0.0")]
      (is (contains? (set (:cwim.core/nodes @(:state updated-map)))
                     {:host "0.0.0.0" :port default-port})))))
