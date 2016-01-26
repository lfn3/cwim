(ns cwim.core
  (:require [org.httpkit.server :as hs]
            [org.httpkit.client :as hc]
            [clojure.core.async :as a]))

;TODO: implement me!
(defn validate-req
  "Check the msg is from the current epoch"
  ([srv msg]
    true))

(defn make-ping-handler [srv]
  "Handler to recv ping messages from other nodes and add updates to state"
  (fn [req]
    (when (validate-req srv (:body req))
      (let [messages (get-in req [:body :messages])]
        (swap! (:state srv) #(apply assoc %1 (apply concat messages)))))))

(defn make-hello-handler [srv])

(defn make-indirect-ping-handler [srv])

(defn move-elem-to-end-of-coll [coll elem]
  (-> coll
      ((partial remove #(= %1 elem)))
      (concat [elem])))

(defn send-impl [target msgs {:keys [epoch nodes]}]
  (let [epoch @epoch]
    (hc/post target
             msgs
             (fn [res]
               (swap! nodes move-elem-to-end-of-coll target)))))

(defn ping
  ([{:keys [state internal-state cfg] :as srv}]
   (let [internal-state @internal-state
         target-nodes (take (:gossip-send-count cfg) (:nodes internal-state))
         msgs-to-send (-> (if-let [max (:max-msgs-per-epoch cfg)]
                            (take max (:send-queue internal-state))
                            (:send-queue internal-state)))]
     (swap! (:interal-state srv) #(vec (drop (count msgs-to-send) %1))) ;Remove messages we're about to send from server state
     (dorun (map #(send-impl %1 msgs-to-send srv) target-nodes)))))

(defn indirect-ping [node])

(defn suspect [node])

(defn dead [node])

(defn swap-return-atom! [atom f & xs]
  (apply swap! atom f xs)
  atom)

(def default-port 64321)
(def srv-map {:self       {:host nil
                           :port nil}
              :cfg        {:max-msgs-per-epoch nil
                           :gossip-send-count  5
                           :ping-time-in-ms    5000
                           :send-fn            (fn [host msgs srv-map] nil)} ;TODO implement me!
              :internal-state (atom {:nodes '() ; ::nodes {:host "0.0.0.0" :port 1234}
                                     :send-queue []
                                     :epoch 0})
              :state      (atom {})
              :shutdown   (fn [] (future nil))})            ;TODO implement me!

;; Public interface

(defn start
  "Starts a server to listen for gossip messages, and for holding cfg.
  Hang onto the return from this, you'll need it when discovering nodes or sending.
  Recommend you shove it in an atom."
  ([] (start default-port))
  ([port]
   (let [srv-map (merge srv-map {:self {:host "0.0.0.0"
                                        :port port}})
         srv (hs/run-server (make-ping-handler srv-map) {:port port})
         ping-time (get-in srv-map [:cfg :ping-time-in-ms])]
     (a/go-loop []
       (try
         (ping srv)
         (catch Exception e))
       (a/<! (a/timeout ping-time))
       (recur)))))

(defn add-node
  "Add another node to gossip to"
  ([srv host] (add-node srv host default-port))
  ([srv host port] (-> srv
                     (update :state swap-return-atom! update ::nodes conj {:host host :port port})
                     (update :state swap-return-atom! update ::nodes shuffle))))

(defn send-msg
  ([srv key val]
   (-> srv
       (update :send-queue conj [key val])
       (assoc-in [:state key] val))))

;TODO: replace this by making srv return state when derefed.
(defn query
  ([srv key]
    (get-in srv [:state key])))
