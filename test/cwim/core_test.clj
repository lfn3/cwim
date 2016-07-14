(ns cwim.core-test
  (:require [clojure.test :refer :all]
            [cwim.core :refer :all]
            [clojure.core.async :as a]
            [org.httpkit.server :as hs]
            [clojure.edn :as edn]))

(def short-time-cfg (assoc default-cfg :ping-timer 500))

(deftest add-node-test
  (testing "Add a single node"
    (let [updated-map (add-node (srv-map) "0.0.0.0")]
      (is (get (:nodes @(:internal-state updated-map))
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
          cfg (merge short-time-cfg {:send-fn    (fn [_ _] (swap! call-count inc))})
          srv (start cfg)]
      (add-node srv "0.0.0.0")
      (a/<!! (a/timeout (* 3 (:ping-timer cfg))))
      (stop srv)
      (is (< 0 @call-count))))

  (testing "Ping stops after shutdown"
    (let [call-count (atom 0)]
      (with-redefs [ping (fn [srv] (swap! call-count inc))]
        (let [cfg short-time-cfg
              srv (start cfg)]
          (a/<!! (a/timeout (* 3 (:ping-timer cfg))))
          (let [current-count @call-count]
            (stop srv)
            (a/<!! (a/timeout (* 3 (:ping-timer cfg))))
            (is (or (= current-count @call-count)
                    (= (inc current-count) @call-count)))   ;possible for one more ping to occur
            (is (< 0 @call-count))))))))

(deftest data-exchange
  (testing "From node gets added"
    (let [srv1 (start short-time-cfg)
          srv2 (start (assoc short-time-cfg :port 65445))]
      (add-node srv1 "127.0.0.1" 65445)
      (a/<!! (a/timeout (* 2 (:ping-timer short-time-cfg))))
      (is (= (first (keys (nodes srv2))) {:host (get-in srv1 [:cfg :host])
                                          :port (get-in srv1 [:cfg :port])}))
      (stop srv1)
      (stop srv2)))
  (testing "Add messages propagate"
    (let [srv1 (start short-time-cfg)
          srv2 (start (assoc short-time-cfg :port 65445))
          srv3 (start (assoc short-time-cfg :port 65446))]
      (add-node srv1 "127.0.0.1" 65445)
      (add-node srv2 "127.0.0.1" 65446)
      (a/<!! (a/timeout (* 3 (:ping-timer short-time-cfg))))
      (is (= (set (keys (nodes srv3)))
             (set [(select-keys (:cfg srv1) [:host :port])
                   (select-keys (:cfg srv2) [:host :port])])))
      (stop srv1)
      (stop srv2)
      (stop srv3)))
  (testing "Messages sent to other nodes"
    (let [srv1 (start short-time-cfg)
          srv2 (start (assoc short-time-cfg :port 65445))]
      (add-node srv1 "127.0.0.1" 65445)
      (send-msg srv1 :test :hello)
      (a/<!! (a/timeout (* 3 (:ping-timer short-time-cfg))))
      (is (= (query srv2 :test)
             :hello))
      (stop srv1)
      (stop srv2)))
  (testing "Stopped nodes are dropped"))
