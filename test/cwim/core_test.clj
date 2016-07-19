(ns cwim.core-test
  (:require [clojure.test :refer :all]
            [cwim.core :refer :all]
            [clojure.core.async :as a]
            [org.httpkit.server :as hs]
            [clojure.edn :as edn]))

(defn test-srv-cfg []
  (-> default-cfg
      (assoc :ping-timer 200)
      (assoc :ack-timeout 300)
      (assoc :suspect-timeout 500)
      (assoc :port (+ 1024 (rand-int 65511)))))

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
          cfg (merge (test-srv-cfg) {:send-fn    (fn [_ _] (swap! call-count inc))})
          srv (start cfg)]
      (add-node srv "0.0.0.0")
      (a/<!! (a/timeout (* 3 (:ping-timer cfg))))
      (stop srv)
      (is (< 0 @call-count))))

  (testing "Ping stops after shutdown"
    (let [call-count (atom 0)]
      (with-redefs [ping (fn [_] (swap! call-count inc))]
        (let [cfg (test-srv-cfg)
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
    (let [srv1 (start (test-srv-cfg))
          srv2 (start (test-srv-cfg))]
      (add-node-from-srv srv1 srv2)
      (a/<!! (a/timeout (* 2 (:ping-timer (test-srv-cfg)))))
      (is (= (first (keys (nodes srv2))) {:host (get-in srv1 [:cfg :host])
                                          :port (get-in srv1 [:cfg :port])}))
      (stop srv1)
      (stop srv2)))
  (testing "Add messages propagate"
    (let [srv1 (start (test-srv-cfg))
          srv2 (start (test-srv-cfg))
          srv3 (start (test-srv-cfg))]
      (add-node-from-srv srv1 srv2)
      (add-node-from-srv srv2 srv3)
      (a/<!! (a/timeout (* 3 (:ping-timer (test-srv-cfg)))))
      (is (= (set (keys (nodes srv3)))
             (set [(select-keys (:cfg srv1) [:host :port])
                   (select-keys (:cfg srv2) [:host :port])])))
      (stop srv1)
      (stop srv2)
      (stop srv3)))
  (testing "Messages sent to other nodes"
    (let [srv1 (start (test-srv-cfg))
          srv2 (start (test-srv-cfg))]
      (add-node-from-srv srv1 srv2)
      (send-msg srv1 :test :hello)
      (a/<!! (a/timeout (* 3 (:ping-timer (test-srv-cfg)))))
      (is (= (query srv2 :test)
             :hello))
      (stop srv1)
      (stop srv2)))
  (testing "Stopped nodes are dropped"
    (let [srv1 (start (test-srv-cfg))
          srv2 (start (test-srv-cfg))]
      (add-node-from-srv srv1 srv2)
      (a/<!! (a/timeout (* 3 (:ping-timer (test-srv-cfg)))))
      (stop srv1)
      (a/<!! (a/timeout (* 3 (:suspect-timeout (test-srv-cfg)))))
      (is (empty? (nodes srv2)))
      (stop srv2))))
