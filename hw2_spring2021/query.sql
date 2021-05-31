SELECT DISTINCT(q.id)
FROM Query AS q,QueryOnDisk AS qod, (SELECT C.query_id AS id, COUNT(*) 
										FROM (SELECT qod.query_id AS query_id ,qod.disk_id AS disk_id
												FROM QueryOnDisk AS qod
												WHERE qod.query_id<>1 AND qod.disk_id IN (SELECT qod.disk_id
																							FROM QueryOnDisk AS qod
																							WHERE qod.query_id=1)) AS C 
										GROUP BY C.query_id
										HAVING COUNT(*)  >= (SELECT COUNT(qod.disk_id) 
															FROM QueryOnDisk AS qod
															WHERE qod.query_id=1
															)/2.0
										ORDER BY C.query_id ASC LIMIT 10) AS q2
WHERE q.id<>1 AND(q.id=q2.id OR 0 = (SELECT COUNT(qod.disk_id) 
					FROM QueryOnDisk AS qod
					WHERE qod.query_id=1))
ORDER BY q.id ASC LIMIT 10