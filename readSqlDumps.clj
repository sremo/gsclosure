(use 'cascalog.ops)
(use 'cascalog.vars)
(use 'cascalog.api)
(use 'cascalog.io)
(use 'clj-time.core)
(use 'clj-time.format)


(use 'clj-time.coerce)
(use 'clj-time.local)

(comment
deprecated

(defn safe-parse-datetime [val] 
	(try (clj-time.format/parse 
		(clj-time.format/formatter "yyy-MM-dd HH:mm:ss") val) 
	(catch Exception _ val)))
 
 
  (defn int-date-string [val] 
  	(safe-parse-datetime ( 
  		#(if (re-matches #"[0-9]*" %) (Integer/parseInt %) %) val)))


)




(defn parse-datetime [val] 
	(clj-time.format/parse (clj-time.format/formatter "yyy-MM-dd HH:mm:ss") val))
 
 (defn safe-parse-datetime [val]
 	(try (parse-datetime val) 
 	(catch Exception _ val)
 	)
 )
 
 
 
 (defn parse-int [val] 
 	(if (re-matches #"[0-9]*" val) (Integer/parseInt val) val) )
 
 (defn int-date-string [val] 
  	(safe-parse-datetime (parse-int val)))

 
   (defmapop safe-split-and-cast [line] 
 		seq (map int-date-string
 		(re-seq #"[^\t]+" line) ) )       





(defn premium_contracts_data [dir]
  (let [source (hfs-textline dir)]
    (<- [?id ?payer_id ?payer_type ?premium_plan_id ?payment_id ?payment_type ?status_code ?expired_at ?created_at ?updated_at ?nth_valid ?cashed_amount] (source ?line)
    (safe-split-and-cast ?line :> ?id ?payer_id ?payer_type ?premium_plan_id ?payment_id ?payment_type ?status_code ?expired_at ?created_at ?updated_at ?nth_valid ?cashed_amount)
                     (:distinct false))))
                     
(defn users_data [dir]
  (let [source (hfs-textline dir)]
    (<- [?id ?username ?sharable_identifier ?name ?is_deleted ?is_publisher ?icon_image_id ?interface_language_id ?native_language_id ?weekly_study_target ?groups_count ?derived_timezone ?password_hash ?salt ?auth_token ?failed_logins ?last_logged_in_at ?studiable_updated_at ?referer ?created_at ?updated_at ?external_id] (source ?line)
    (safe-split-and-cast ?line :> ?id ?username ?sharable_identifier ?name ?is_deleted ?is_publisher ?icon_image_id ?interface_language_id ?native_language_id ?weekly_study_target ?groups_count ?derived_timezone ?password_hash ?salt ?auth_token ?failed_logins ?last_logged_in_at ?studiable_updated_at ?referer ?created_at ?updated_at ?external_id)
                     (:distinct false))))	                     
                     
	
	
(defn low_id [dir]
	(let [premium_contracts (premium_contracts_data dir)] 
    	(?<- (stdout) [?cnt] 
        (premium_contracts ?id ?payer_id _ _ _ _ _ _ _ ?updated_at _ _) 
        (before? ?updated_at (parse-datetime "2011-01-21 15:00:00"))
        (count ?cnt)
        )
    )
)



(low_id "/Users/rstorni/Desktop/premium_contracts.txt")	     