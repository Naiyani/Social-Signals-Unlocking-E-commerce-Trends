insert into final_feedback
select * from feedback where md5_hash NOT IN (
    select md5_hash from final_feedback
);

insert into feedback_duplicate_archive
select * from feedback where md5_hash IN (
    select md5_hash from final_feedback
)
AND feedback_id NOT IN ( 
    select feedback_id FROM final_feedback
);
