create table staging.prototype_data as
select ld1.value as part_description1, q.entity1_id, ld2.value as part_description2, q.entity2_id,  q.human_match_prob
from questions q, local_data ld1, local_data ld2
where q.entity1_id = ld1.entity_id and q.entity2_id = ld2.entity_id
and ld1.field_id in (196,220,79,112)
and ld2.field_id in (196,220,79,112);