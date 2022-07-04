with test_result as (
  select * from public_test.dm_settlement_report_expected
  except
  select * from public_test.dm_settlement_report_actual
)
insert into public_test.testing_result (test_date_time, test_name, test_result)
  select
      NOW(),
      'test_01',
      case 
          when count(*) = 0 then true
          else false 
      end
from test_result;

-- Двигайтесь дальше! Ваш код: ngY7uVwwuM
