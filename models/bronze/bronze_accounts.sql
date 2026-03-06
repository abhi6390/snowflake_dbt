select * 
from {{ source('banking', 'raw_accounts') }}