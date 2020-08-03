create database if not exists  wt_hbase_gy_test;
use wt_hbase_gy_test;

drop table if exists cal_record;
create external table cal_record (
primary_key string comment "主键cal_date",
init_date string comment "如果当天任务跑完，则往init_date里插入最新一天跑完数据的日期"
)
comment "存放后台标签是否跑完的标志位的表"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date")  
tblproperties("hbase.table.name"="zhfx_gy_test:cal_record");


-- 普通用户每天数据
-- fund_account = reverse(fund_account)
-- cal_date = 99999999 - init_date
drop table if exists user_daily_data;
create external table user_daily_data(
primary_key string comment "主键fund_account,cal_date",
init_date string comment "时间",
total_asset decimal(19,2) comment "账户每日总资产",
daily_income decimal(19,2) comment "账户日盈亏",
cash_asset decimal(19,2) comment "账户现金资产",
stock_market_value decimal(19,2) comment "账户股票市值 股票+场内基金",
stock_day_income decimal(19,2) comment "账户股票日盈亏 股票+场内基金",
bond_market_value decimal(19,2) comment "账户债券市值",
bond_day_income decimal(19,2) comment "账户债券日盈亏",
other_market_value decimal(19,2) comment "账户其他市值",
other_day_income decimal(19,2) comment "账户其他日盈亏",
outside_market_value decimal(19,2) comment "场外市值",
outside_income decimal(19,2) comment "场外盈亏",
otc_market_value decimal(19,2) comment "理财市值",
otc_income decimal(19,2) comment "理财盈亏",
daily_income_ratio decimal(19,4) comment "账户日收益率",
day_position decimal(19,4) comment "账户每日仓位"
)
comment "普通用户每天数据"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:total_asset,tag_base:daily_income,tag_base:cash_asset,tag_base:stock_market_value,tag_base:stock_day_income,tag_base:bond_market_value,tag_base:bond_day_income,tag_base:other_market_value,tag_base:other_day_income,tag_base:outside_market_value,tag_base:outside_income,tag_base:otc_market_value,tag_base:otc_income,tag_base:daily_income_ratio,tag_base:day_position")  
tblproperties("hbase.table.name"="zhfx_gy_test:user_daily_data");


-- 大盘日数据
drop table if exists market_daily_data;
create external table market_daily_data(
primary_key string comment "主键cal_date",
init_date string comment "时间",
sh_day_yield decimal(19,4) comment "上证指数日收益率",
sz_day_yield decimal(19,4) comment "深证指数日收益率",
gem_day_yield decimal(19,4) comment "创业板日收益率",
hs300_day_yield decimal(19,4) comment "沪深300日收益率",
sz50_day_yield decimal(19,4) comment "上证50日收益率",
zz500_day_yield decimal(19,4) comment "中证500日收益率"
)
comment "大盘日数据"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:sh_day_yield,tag_base:sz_day_yield,tag_base:gem_day_yield,tag_base:hs300_day_yield,tag_base:sz50_day_yield,tag_base:zz500_day_yield")  
tblproperties("hbase.table.name"="zhfx_gy_test:market_daily_data");


-- 大盘区间累计数据
drop table if exists market_cumulative_data;
create external table market_cumulative_data(
primary_key string comment "主键:interval_type,init_date",
init_date string comment "时间",
ac_sh_yield decimal(19,4) comment "上证指数累计收益率",
ac_sz_yield decimal(19,4) comment "深证指数累计收益率",
ac_gem_yield decimal(19,4) comment "创业板累计收益率",
ac_hs300_yield decimal(19,4) comment "沪深300累计收益率",
ac_sz50_yield decimal(19,4) comment "上证50累计收益率",
ac_zz500_yield decimal(19,4) comment "中证500累计收益率"
)
comment "大盘区间累计数据"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:ac_sh_yield,tag_base:ac_sz_yield,tag_base:ac_gem_yield,tag_base:ac_hs300_yield,tag_base:ac_sz50_yield,tag_base:ac_zz500_yield")  
tblproperties("hbase.table.name"="zhfx_gy_test:market_cumulative_data");


-- fund_account = reverse(fund_account)
-- 用户月数据
drop table if exists user_month_data;
create external table user_month_data(
primary_key string comment "主键:fund_account,init_month",
init_month string comment "月份",
asset_month_income decimal(19,2) comment "账户月收益",
hs300_month_yield decimal(19,4) comment "沪深300月收益率",
asset_year_income decimal(19,2) comment "賬戶年收益",
hs300_year_yield decimal(19,4) comment "滬深300年收益率"

)
comment "用户月数据"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_month,tag_base:asset_month_income,tag_base:hs300_month_yield,tag_base:asset_year_income,tag_base:hs300_year_yield")  
tblproperties("hbase.table.name"="zhfx_gy_test:user_month_data");

-- select concat(${hivevar:endDate},",",fund_account, ",", interval_type,",0") as primary_key,
-- fund_account = reverse(fund_account)
-- interval_type 1 近一月，2 近三月 3 近半年 4 近一年 9 今年 8 當月
-- asset_prop 0 普通用戶  7 信用用戶
-- cal_date = 99999999 - init_date
-- 用户区间数据
drop table if exists user_interval_data;
create external table user_interval_data(
primary_key string comment "主键:fund_account,interval_type,asset_prop,cal_date",
init_date string comment "时间",
asset_income decimal(19,2) comment "账户区间收益",
asset_yield decimal(19,2) comment "账户区间收益率",
asset_yield_rank decimal(19,2) comment "账户区间收益率排名百分比",
stock_income decimal(19,2) comment "账户区间股票盈亏 股票+场内基金",
bond_income decimal(19,2) comment "账户区间债券盈亏",
other_income decimal(19,2) comment "账户区间其他盈亏",
begin_asset decimal(19,2) comment "账户期初净资产",
begin_date string comment "期初日期",
fund_in decimal(19,2) comment "账户区间转入",
fund_out decimal(19,2) comment "账户区间转出",
end_date string comment "期末日期",
end_asset decimal(19,2) comment "账户期末净资产",
stock_count int comment "交易股票数",
profit_count int comment "股票盈利数量",
buy_count int comment "买入次数",
sell_count int comment "卖出次数",
avg_hold_day int comment "个股平均持股天数",
win_ratio decimal(19,4) comment "交易胜率",
stock_profit decimal(19,2) comment "股票交易盈利",
stock_loss decimal(19,2) comment "股票交易亏损",
stock_business_balance decimal(19,2) comment "股票交易额",
trade_frequency decimal(19,2) comment "交易频率",
avg_hold_day_rank decimal(19,4) comment "平均持股天数超过股民百分比",
avg_market_value decimal(19,4) comment "平均股票市值占比",
avg_market_value_rank decimal(19,4) comment "平均股票市值占比超过股民百分比",
win_ratio_rank decimal(19,4) comment "个股胜率超过股民百分比",
draw_back decimal(19,4) comment "最大回撤",
draw_back_rank decimal(19,4) comment "最大回撤超过股民百分比"

)
comment "用户区间数据"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:asset_income,tag_base:asset_yield,tag_base:asset_yield_rank,tag_base:stock_income,tag_base:bond_income,tag_base:other_income,tag_base:begin_asset,tag_base:begin_date,tag_base:fund_in,tag_base:fund_out,tag_base:end_date,tag_base:end_asset,tag_base:stock_count,tag_base:profit_count,tag_base:buy_count,tag_base:sell_count,tag_base:avg_hold_day,tag_base:win_ratio,tag_base:stock_profit,tag_base:stock_loss,tag_base:stock_business_balance,tag_base:trade_frequency,tag_base:avg_hold_day_rank,tag_base:avg_market_value,tag_base:avg_market_value_rank,tag_base:win_ratio_rank,tag_base:draw_back,tag_base:draw_back_rank")  
tblproperties("hbase.table.name"="zhfx_gy_test:user_interval_data");

-- 交易分析页面
-- fund_account = reverse(fund_account)
-- interval_type 1 近一月，2 近三月 3 近半年 4 近一年 9 今年 8 當月
-- cal_date = 99999999 - init_date
-- trade_type string comment "账户交易类型,0:普通,1:融资,2:融券",
drop table if exists interval_stock;
create external table interval_stock(
primary_key string comment "主键fund_account,interval_type,trade_type,cal_date",
stock_content string comment "stock_code,stock_name,stock_type,exchange_type,hold_income,income_percent,hold_day,amount,hold_status,money_type"
)
comment "交易分析页面"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:stock_content")  
tblproperties("hbase.table.name"="zhfx_gy_test:interval_stock");

-- 交易分布数据
-- 建议  cal_date,fund_account,interval_type,trade_type 做主键，其他的存为json格式数据
drop table if exists interval_trade_distribution;
create external table interval_trade_distribution (
primary_key string comment "主键fund_account,interval_type,trade_type,cal_date",
distribute_content string comment "distribute_name,distribute_type,distribute_mode,distribute_value"
)
comment "交易分布数据"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:distribute_content")  
tblproperties("hbase.table.name"="zhfx_gy_test:interval_trade_distribution");


--第二批，交易详情
-- 历史交割流水表
-- 建议主键 fund_account,init_date,stock_code,exchange_type 其余字段以json格式保存
drop table if exists his_deliver;
create external table his_deliver(
primary_key string comment "主键fund_account,init_date,stock_code,exchange_type",
deliver_content string comment "字段顺序：init_date,business_time,serial_no,business_flag,business_amount,post_amount,business_price,business_balance,money_type"
)
comment "历史交割流水表"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:deliver_content")  
tblproperties("hbase.table.name" = "zhfx_gy_test:his_deliver");
