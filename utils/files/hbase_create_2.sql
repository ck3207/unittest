create schema if not exists wt_hbase_chenk_gl;
use wt_hbase_chenk_gl;

drop table if exists cal_record;
create external table cal_record (
primary_key string comment "主键cal_date",
init_date string comment "如果当天任务跑完，则往init_date里插入最新一天跑完数据的日期"
)
comment "存放后台标签是否跑完的标志位的表"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:cal_record");


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
tblproperties("hbase.table.name" = "zhfx_gl_chenk:his_deliver");



-- 普通用户每天资产数据（包括人民币、港元、美元）
drop table if exists user_daily_asset;
create external table user_daily_asset(
primary_key string comment "主键fund_account,init_date",
init_date string comment "时间",
begin_RMB_asset decimal(19,2) comment "修正前人民币资产",
end_RMB_asset decimal(19,2) comment "修正后人民币资产",
begin_HKD_asset decimal(19,2) comment "修正前港元资产",
end_HKD_asset decimal(19,2) comment "修正后港元资产",
begin_doller_asset decimal(19,2) comment "修正前美元资产",
end_doller_asset decimal(19,2) comment "修正后美元资产"
)
comment "普通用户每天资产数据（包括人民币、港元、美元）"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:begin_RMB_asset,tag_base:end_RMB_asset,tag_base:begin_HKD_asset,tag_base:end_HKD_asset,tag_base:begin_doller_asset,tag_base:end_doller_asset")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:user_daily_asset");


-- 普通用户每天数据
drop table if exists user_daily_data;
create external table user_daily_data(
primary_key string comment "主键fund_account,init_date",
-- 日数据，画曲线的
-- 获取月账单曲线数据:MonthBillDateDailyData
init_date string comment "时间",
daily_income decimal(19,2) comment "每日盈亏金额",
daily_income_ratio decimal(19,4) comment "每日收益率",
total_asset decimal(19,2) comment "每日总资产",
in_balance decimal(19,2) comment "每日资金转入",
out_balance decimal(19,2) comment "每日资金转出",
hs_daily_income_ratio decimal(19,4) comment "沪深300当日收益率",
position decimal(19,4) comment "每日仓位",
daily_income_ratio_percent decimal(19,4) comment "每日收益率超过股民百分比",
trade_balance decimal(19,2) comment "每日交易金额",
single_stock_content string comment "个股日盈亏额（stock_code,exchange_type,stock_name,asset_price,single_stock_day_income,single_stock_day_ratio,business_status(1建仓，2加仓，3减仓，4清仓)）",
deliver_content string comment "交易记录（stock_code,exchange_type,stock_name,init_date,business_time,business_amount,business_price,business_balance,business_flag,business_status(1建仓，2加仓，3减仓，4清仓)）"
)
comment "普通用户每天数据"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:daily_income,tag_base:daily_income_ratio,tag_base:total_asset,tag_base:in_balance,tag_base:out_balance,tag_base:hs_daily_income_ratio,tag_base:position,tag_base:daily_income_ratio_percent,tag_base:trade_balance,tag_base:single_stock_content,tag_base:deliver_content")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:user_daily_data");




-- 信用用户每天数据
drop table if exists credit_user_daily_data;
create external table credit_user_daily_data(
primary_key string comment "主键fund_account,init_date",
init_date string comment "时间",
total_asset decimal(19,2) comment "总资产",
net_asset decimal(19,2) comment "净资产--C7",
income decimal(19,2) comment "日收益",
yield decimal(19,4) comment "日收益率",
assure_ratio decimal(19,4) comment "担保比例-C7",
hs_daily_income_ratio decimal(19,4) comment "沪深300当日收益率"
)
comment "信用用户每天数据"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:total_asset,tag_base:net_asset,tag_base:income,tag_base:yield,tag_base:assure_ratio,tag_base:hs_daily_income_ratio")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:credit_user_daily_data");


-- 原中台表 指数区间累计收益率 interval_index_ac_yield + 投资能力得分 interval_invest_ability
drop table if exists basic_data;
create external table basic_data(
primary_key string comment "主键:interval_type,init_date",
init_date string comment "时间",
ac_sh_yield decimal(19,4) comment "上证累计收益率",
ac_sz_yield decimal(19,4) comment "深圳累计收益率",
ac_gem_yield decimal(19,4) comment "创业板累计收益率",
ac_hs_yield decimal(19,4) comment "沪深300累计收益率",
trans_ability decimal(19,2) comment "综合评分",
income_ability decimal(19,2) comment "盈利能力",
timing_ability decimal(19,2) comment "择时能力",
risk_control_ability decimal(19,2) comment "风控能力",
discipline_ability decimal(19,2) comment "交易纪律",
choose_ability decimal(19,2) comment "选股能力"
)
comment "基础数据：指数数据和全体用户平均值"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:ac_sh_yield,tag_base:ac_sz_yield,tag_base:ac_gem_yield,tag_base:ac_hs_yield,tag_base:trans_ability,tag_base:income_ability,tag_base:timing_ability,tag_base:risk_control_ability,tag_base:discipline_ability,tag_base:choose_ability")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:basic_data");



-- 原中台表 指数区间累计收益率 interval_index_ac_yield + 投资能力得分 interval_invest_ability
drop table if exists credit_basic_data;
create external table credit_basic_data(
primary_key string comment "主键:interval_type,init_date",
init_date string comment "时间",
ac_sh_yield decimal(19,4) comment "上证累计收益率",
ac_sz_yield decimal(19,4) comment "深圳累计收益率",
ac_gem_yield decimal(19,4) comment "创业板累计收益率",
ac_hs_yield decimal(19,4) comment "沪深300累计收益率",
trans_ability decimal(19,2) comment "综合评分",
income_ability decimal(19,2) comment "盈利能力",
timing_ability decimal(19,2) comment "择时能力",
risk_control_ability decimal(19,2) comment "风控能力",
discipline_ability decimal(19,2) comment "交易纪律",
choose_ability decimal(19,2) comment "选股能力"
)
comment "基础数据：指数数据和全体用户平均值"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:ac_sh_yield,tag_base:ac_sz_yield,tag_base:ac_gem_yield,tag_base:ac_hs_yield,tag_base:trans_ability,tag_base:income_ability,tag_base:timing_ability,tag_base:risk_control_ability,tag_base:discipline_ability,tag_base:choose_ability")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:credit_basic_data");


-- 原中台表 daily_index
drop table if exists daily_basic_data;
create external table daily_basic_data(
primary_key string comment "主键:init_date",
init_date string comment "时间",
sh_price decimal(19,3) comment "上证收盘价",
sh_yield decimal(19,4) comment "上证收益率",
sz_price decimal(19,3) comment "深圳收盘价",
sz_yield decimal(19,4) comment "深圳收益率",
gem_price decimal(19,3) comment "创业板收盘价",
gem_yield decimal(19,4) comment "创业板收益率",
hs_price decimal(19,3) comment "沪深300收盘价",
hs_yield decimal(19,4) comment "沪深300收益率"
)
comment "基础数据：每日指数数据"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:init_date,tag_base:sh_price,tag_base:sh_yield,tag_base:sz_price,tag_base:sz_yield,tag_base:gem_price,tag_base:gem_yield,tag_base:hs_price,tag_base:hs_yield")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:daily_basic_data");


-- 累计收益率、超越基金经理中台算
-- 我的收益率、超hs300指数比例：画完线后，中台或者前端减（累计收益率是中台算的，确保数据对的平）
-- 前端计算：持股成功率
-- 流入流出需要与每日数据区分开来，这里是区间值，沿用原来命名
-- select concat(${hivevar:endDate},",",fund_account, ",", interval_type,",0") as primary_key,
drop table if exists home_page_data;
create external table home_page_data(
primary_key string comment "主键:fund_account,interval_type,asset_prop,cal_date",
-- 普通账户标签
asset_income decimal(19,2) comment "账户盈亏",
stock_income decimal(19,2) comment "股票盈亏-G0",
otc_income decimal(19,2) comment "基金理财盈亏-G0",
bond_income decimal(19,2) comment "债券盈亏-G0",
bshare_income decimal(19,2) comment "B股盈亏-G0",
other_assets_income decimal(19,2) comment "其他盈亏-G0",

-- 信用标签
assure_income decimal(19,2) comment "担保交易收益-C7",
loan_income decimal(19,2) comment "两融交易收益-C7",
fare decimal(19,2) comment "其他利息费用-C7",
assure_yield decimal(19,4) comment "担保/融资交易收益率-C7",
loan_yield decimal(19,4) comment "融券交易收益率-C7",
begin_debt decimal(19,2) comment "期初负债-C7",
new_debt decimal(19,2) comment "新增负债-C7",
return_debt decimal(19,2) comment "归还负债-C7",
debt_change decimal(19,2) comment "负债变动-C7",
last_debt decimal(19,2) comment "期末负债-C7",
slo_stock_count int comment "（融券交易）交易股票个数",
slo_profit_count int comment "（融券交易）盈利个数",
slo_win_ratio decimal(19,4) comment "胜率",
slo_win_ratio_rank decimal(19,4) comment "（融券交易）超过兴证全体客户占比",
higher_bail_count int comment "高于取保线天数-C7",
lower_bail_count int comment "低于取保线天数-C7",
lower_vigilance_count int comment "低于警戒线天数-C7",
hit_flat_count int comment "触碰平仓线天数-C7",

-- 共有标签
begin_date string comment "期初时间",
begin_asset decimal(19,2) comment "期初资产",
fund_in decimal(19,2) comment "转入",
fund_out decimal(19,2) comment "转出",
last_asset decimal(19,2) comment "期末资产",
end_date string comment "期末时间",
stock_yield decimal(19,4) comment "股票收益率",
stock_count int comment "（股票交易）交易股票个数",
profit_count int comment "（股票交易）盈利个数",
win_ratio decimal(19,4) comment "胜率",
win_ratio_rank decimal(19,4) comment "（股票交易）超过兴证全体客户占比",

first_profit string comment "盈利top3第一（stock_name,stock_code,income,income_rate,trade_type）",
second_profit string comment "盈利top3第二（股票名称，股票代码，盈亏金额，占比，是否融券）",
third_profit string comment "盈利top3第三（股票名称，股票代码，盈亏金额，占比，是否融券）",
first_loss string comment "亏损top3第一（股票名称，股票代码，盈亏金额，占比，是否融券）",
second_loss string comment "亏损top3第二（股票名称，股票代码，盈亏金额，占比，是否融券）",
third_loss string comment "亏损top3第三（股票名称，股票代码，盈亏金额，占比，是否融券）",
trans_ability decimal(19,2) comment "综合评分",
income_ability decimal(19,2) comment "盈利能力",
timing_ability decimal(19,2) comment "择时能力",
discipline_ability decimal(19,2) comment "交易纪律",
choose_ability decimal(19,2) comment "选股能力",
risk_control_ability decimal(19,2) comment "风控能力",
ability_rank decimal(19,4) comment "得分排名",
stock_exhcange_right decimal(19,4) comment "股票交易权限"
)
comment "用户首页"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:asset_income,tag_base:stock_income,tag_base:otc_income,tag_base:bond_income,tag_base:bshare_income,tag_base:other_assets_income,tag_base:assure_income,tag_base:loan_income,tag_base:fare,tag_base:assure_yield,tag_base:loan_yield,tag_base:begin_debt,tag_base:new_debt,tag_base:return_debt,tag_base:debit_change,tag_base:last_debt,tag_base:slo_stock_count,tag_base:slo_profit_count,tag_base:slo_win_ratio,tag_base:slo_win_ratio_rank,tag_base:higher_bail_count,tag_base:lower_bail_count,tag_base:lower_vigilance_count,tag_base:hit_flat_count,tag_base:begin_date,tag_base:begin_asset,tag_base:fund_in,tag_base:fund_out,tag_base:last_asset,tag_base:end_date,tag_base:stock_yield,tag_base:stock_count,tag_base:profit_count,tag_base:win_ratio,tag_base:win_ratio_rank,tag_base:first_profit,tag_base:second_profit,tag_base:third_profit,tag_base:first_loss,tag_base:second_loss,tag_base:third_loss,tag_base:trans_ability,tag_base:income_ability,tag_base:timing_ability,tag_base:discipline_ability,tag_base:choose_ability,tag_base:risk_control_ability,tag_base:ability_rank,tag_base:stock_exhcange_right")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:home_page_data");

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
tblproperties("hbase.table.name"="zhfx_gl_chenk:trade_distribution");

-- 交易分析页面
drop table if exists trade_statistics;
create external table trade_statistics(
primary_key string comment "主键:fund_account,interval_type,trade_type,cal_date",
stock_name string comment "持股偏好-股票名称",
stock_code string comment "持股偏好-股票代码",
stock_type string comment "持股偏好-股票类型",
exchange_type string comment "持股偏好-市场类型",
hold_day int comment "持股偏好-持有天数",
income decimal(19,4) comment "持股偏好-盈亏金额",
trade_balance decimal(19,4) comment "股票/融券交易额",
stock_count int comment "交易证券数",
buy_amount int comment "买入次数",
sell_amount decimal(19,4) comment "卖出次数",
trade_frequency decimal(19,4) comment "交易频率（笔/天）",
avg_hold_day int comment "平均持股天数",
avg_hold_day_rank decimal(19,4) comment "平均持股天数超过股民占比",
avg_market_value decimal(19,4) comment "平均股票市值占比",
avg_market_value_rank decimal(19,4) comment "平均股票市值占比超过股民占比",
avg_assure_ratio decimal(19,4) comment "平均担保比例",
avg_assure_ratio_rank decimal(19,4) comment "平均担保比例超过股民占比",
win_ratio decimal(19,4) comment "个股胜率",
win_ratio_rank decimal(19,4) comment "个股胜率超过股民占比",
draw_back decimal(19,4) comment "最大回撤",
draw_back_rank decimal(19,4) comment "最大回撤超过股民占比"
)
comment "交易分析页面"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:stock_name,tag_base:stock_code,tag_base:stock_type,tag_base:exchange_type,tag_base:hold_day,tag_base:income,tag_base:trade_balance,tag_base:stock_count,tag_base:buy_amount,tag_base:sell_amount,tag_base:trade_frequency,tag_base:avg_hold_day,tag_base:avg_hold_day_rank,tag_base:avg_market_value,tag_base:avg_market_value_rank,tag_base:avg_assure_ratio,tag_base:avg_assure_ratio_rank,tag_base:win_ratio,tag_base:win_ratio_rank,tag_base:draw_back,tag_base:draw_back_rank")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:trade_statistics");


-- 交易分析页面
-- trade_type string comment "账户交易类型,0:普通,1:融资,2:融券",
drop table if exists interval_stock;
create external table interval_stock(
primary_key string comment "主键fund_account,interval_type,trade_type,cal_date",
stock_content string comment "stock_code,stock_name,stock_type,exchange_type,income,hold_day,amount,hold_status,money_type"
)
comment "交易分析页面"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:stock_content")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:interval_stock");


-- 基金经理收益率
drop table if exists interval_fund_rank;
create external table interval_fund_rank(
primary_key string comment "主键interval_type,cal_date",
fund_content string comment "prod_no,prod_code,yield"
)
comment "基金经理收益率"
stored by "org.apache.hadoop.hive.hbase.HBaseStorageHandler"   
with serdeproperties("hbase.columns.mapping" = ":key,tag_base:fund_content")  
tblproperties("hbase.table.name"="zhfx_gl_chenk:interval_fund_rank");


