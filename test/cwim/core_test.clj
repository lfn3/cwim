(ns cwim.core-test
  (:require [clojure.test :refer :all]
            [cwim.core :refer :all]
            [clojure.core.async :as a]))

(deftest add-node-test
  (testing "Add a single node"
    (let [updated-map (add-node (srv-map) "0.0.0.0")]
      (is (contains? (set (:nodes @(:internal-state updated-map)))
                     {:host "0.0.0.0" :port default-port}))))

  (testing "Adding nodes reshuffles order"
    (let [matching-sort-count (atom 0)
          srv-map (srv-map)]
      (doseq [host (range 60)]
        (add-node srv-map (str "0.0.0." host))
        (when (= (:nodes @(:internal-state srv-map))
                  (sort-by :host (:nodes @(:internal-state srv-map))))
          (swap! matching-sort-count inc)))
      (is (> 4 @matching-sort-count)))))

(deftest start-server-test
  (testing "Send-fn is called on ping"
    (let [call-count (atom 0)
          cfg (merge default-cfg {:send-fn    (fn [_ _ _] (swap! call-count inc))
                                  :ping-timer 500})
          srv (start cfg)]
      (add-node srv "0.0.0.0")
      (a/<!! (a/timeout (* 3 (:ping-timer cfg))))
      (stop srv)
      (is (< 0 @call-count))))

  (testing "Ping stops after shutdown"
    (let [call-count (atom 0)]
      (with-redefs [ping (fn [srv] (swap! call-count inc))]
        (let [cfg (assoc default-cfg :ping-timer 500)
              srv (start cfg)]
          (a/<!! (a/timeout (* 3 (:ping-timer cfg))))
          (let [current-count @call-count]
            (stop srv)
            (a/<!! (a/timeout (* 3 (:ping-timer cfg))))
            (is (= current-count @call-count))
            (is (< 0 @call-count))))))))