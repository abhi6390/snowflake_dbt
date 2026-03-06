select * 
from {{ source('banking', 'raw_daily_balances') }}