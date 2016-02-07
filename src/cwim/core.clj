(ns cwim.core
  (:require [org.httpkit.server :as hs]
            [org.httpkit.client :as hc]
            [clojure.core.async :as a]
            [clojure.edn :as edn]))

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

(defn send-impl [target msg completed-fn]
  (hc/post target
           (pr-str msg)
           (fn [res]
             (if (= 200 (:status res))
               (completed-fn (edn/read-string (:body res)))
               (completed-fn false)))))

(defn indirect-ping [node])

(defn suspect [node]
  ;TODO: add to suspect list
  (indirect-ping node))

(defn create-completion-fn [target internal-state]
  (fn [body]
    (if body
      (swap! internal-state update :nodes move-elem-to-end-of-coll target)
      (suspect target))))

(defn ping
  ([{:keys [state internal-state cfg] :as srv}]
   (let [internal-state @internal-state
         target-nodes (take (:gossip-send-count cfg) (:nodes internal-state))
         msgs-to-send (-> (if-let [max (:max-msgs-per-epoch cfg)]
                            (take max (:send-queue internal-state))
                            (:send-queue internal-state)))
         send-fn (:send-fn cfg)]
     (prn "Pinging to " target-nodes " with " msgs-to-send " using " send-fn)
     ;TODO: replace below with something more robust...
     (swap! (:internal-state srv) #(vec (drop (count msgs-to-send) %1))) ;Remove messages we're about to send from server state
     (dorun (map #(send-fn %1 msgs-to-send (create-completion-fn %1 (:internal-state srv))) target-nodes)))))

(defn dead [node])

(defn swap-return-atom! [atom f & xs]
  (apply swap! atom f xs)
  atom)

(def default-port 64321)

(def default-cfg {:host               "0.0.0.0"
                  :port               default-port
                  :max-msgs-per-epoch nil                   ;nil = unlimted
                  :gossip-send-count  5
                  :ping-timer         5000
                  :send-fn            send-impl             ;(fn [host msgs completion-fn]) -> nil
                  :shutdown-fn        (fn [srv] nil)        ;TODO move this around?
                  })

(defn srv-map [] {:cfg            default-cfg
                  :internal-state (atom {:nodes '() ; ::nodes {:host "0.0.0.0" :port 1234}
                                         :send-queue []
                                         :epoch 0})
                  :state      (atom {})
                  :shutdown   (fn [] (future nil))})            ;TODO implement me!

;; Public interface


(defn start
  "Starts a server to listen for gossip messages, and for holding cfg.
  Hang onto the return from this, you'll need it when discovering nodes or sending.
  Recommend you shove it in an atom (or mount/component)."
  ([] (start {}))
  ([cfg]
   (let [srv-map (assoc (srv-map) :cfg (merge default-cfg cfg))
         srv (hs/run-server (make-ping-handler srv-map) {:port (get-in srv-map [:cfg :port])})
         ping-time (get-in srv-map [:cfg :ping-timer])
         control-ch (a/chan)]
     (a/go-loop []
       (let [[v ch] (a/alts! [(a/timeout ping-time)
                              control-ch])]
         (if (= ch control-ch)
           (srv)
           (do
             (try
               (ping srv-map)
               (catch Exception e
                 ;TODO Error handling strategy
                 (prn e)))
             (recur)))))
     ;TODO shoving this in the cfg map doesn't seem right, precludes using a different server?
     ;TODO put this somwhere else, the shutdown-fn should be produced by the server-start-fn,
     ;TODO and invoked inside the go-loop above.
     (assoc-in srv-map [:cfg :shutdown-fn] (fn [_] (a/put! control-ch :shutdown))))))

(defn stop
  "Stops a running server"
  [srv]
  ((get-in srv [:cfg :shutdown-fn] (fn [_]
                                     (prn "Shutdown not defined, doing nothing...")
                                     nil)) srv))

(defn add-node
  "Add another node to gossip to"
  ([srv host] (add-node srv host default-port))
  ([srv host port] (-> srv
                     (update :internal-state swap-return-atom! update :nodes conj {:host host :port port})
                     (update :internal-state swap-return-atom! update :nodes shuffle))))

(defn send-msg
  ([srv key val]
   (-> srv
       (update :send-queue conj [key val])
       (assoc-in [:state key] val))))

;TODO: replace this by making srv return state when derefed.
(defn query
  ([srv key]
    (get-in srv [:state key])))
