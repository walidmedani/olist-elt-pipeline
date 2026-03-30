-- Fails if any review_score is outside the valid 1-5 range
select
    review_id,
    review_score
from {{ ref('stg_order_reviews') }}
where review_score < 1 or review_score > 5
