select * 
from {{ source('banking', 'raw_transactions') }}