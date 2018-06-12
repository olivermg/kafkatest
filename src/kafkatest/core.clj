(ns kafkatest.core
  (:gen-class)
  (:require [kinsky.client :as kc]
            [kinsky.async :as ka]
            [clojure.core.async :as a :refer [go go-loop <! >! put!]]))


(defn- produce []
  (let [ch (ka/producer {:bootstrap.servers "localhost:9092"} :keyword :edn)]
    (go (>! ch {:topic "testtopic1" :key :aaa :value {:foo :bar}})
        (>! ch {:topic "testtopic1" :key :bbb :value {:bar :baz}}))))


(defn- consume []
  (let [[datach ctrlch] (ka/consumer {:bootstrap.servers "localhost:9092"
                                      :group.id (str (java.util.UUID/randomUUID))}
                                     :keyword :edn)]
    (go-loop []
      (when-let [record (<! datach)]
        (println record)
        (recur)))
    #_(put! ctrlch {:op :partitions-for :topic "testtopic1"})
    (put! ctrlch {:op :subscribe :topic "testtopic1" :type :record})
    #_(put! ctrlch {:op :commit})
    [datach ctrlch]))


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
  (produce)
  (consume))
