#### List of dimensional and fact tables

|table name|fact or dimension table|
|-|-|
|[financial_report_dim](#financial_report_dim)|dimension|
|[hashtags_dim](#hashtags_dim)|dimension|
|[holders_dim](#holders_dim)|dimension|
|[news_dim](#news_dim)|dimension|
|[sector_dim](#sector_dim)|dimension|
|[tickers_news_fact](#tickers_news_fact)|fact|
|[tickers_actions_fact](#tickers_actions_fact)|fact|
|[tickers_analysis_fact](#tickers_analysis_fact)|fact|
|[tickers_balancesheet_fact](#tickers_balancesheet_fact)|fact|
|[tickers_cashflow_fact](#tickers_cashflow_fact)|fact|
|[tickers_earnings_fact](#tickers_earnings_fact)|fact|
|[tickers_eps_fact](#tickers_eps_fact)|fact|
|[tickers_financials_fact](#tickers_financials_fact)|fact|
|[tickers_historical_prices_fact](#tickers_historical_prices_fact)|fact|
|[tickers_holders_fact](#tickers_holders_fact)|fact|
|[tickers_info_dim](#tickers_info_dim)|dimension|
|[tickers_options_fact](#tickers_options_fact)|fact|
|[tickers_recommendations_fact](#tickers_recommendations_fact)|fact|
|[exchanges_dim](#exchanges_dim)|dimension|
|[markets_dim](#markets_dim)|dimension|
|[market_indexes_dim](#market_indexes_dim)|dimension|
|[tickers_shares_fact](#tickers_shares_fact)|fact|
|[tickers_tweets_users_fact](#tickers_tweets_users_fact)|fact|
|[tweets_dim](#tweets_dim)|dimension|
|[user_id_dim](#user_id_dim)|dimension|


FK : Foreign key

PK : Primary key

Foreign table : in case of foreign key, mention of the table source where the field is PK.

<a id = 'financial_report_dim'></a>
#### financial_report_dim

field|	format|	is PK|	is FK |Foreign table|description| value example|
-|-|-|-|-|-|-|
date|String ||||reporting date (YYYY-MM-DD) |2021-09-30|
ticker|String||Y|tickers_info_dim|ticker code|AOS|
period|String||||period related to the report|quarter|
reporting_id|String|Y|||reporting id composed of the combinaison of fields ticker, date and period|AOS_2021-09-30_quarter|

<a id = 'hashtags_dim'></a>
#### hashtags_dim

field|	format|	is PK|	is FK |Foreign table|description| value example|
-|-|-|-|-|-|-|
hashtag|String||||hashtag label|lordofchange|
tweet_id|Integer||Y|tweets_dim|Id of the tweet|1571151560373313539|
hashtag_id|Integer|Y|||Id of the hashtag|18|

<a id = 'holders_dim'></a>		
#### holders_dim

field|	format|	is PK|	is FK |Foreign table|description| value example|
-|-|-|-|-|-|-|
holder|String |Y|||holder name|Schwab Strategic Tr-Schwab U.S. Dividend Equity ETF|
holder_type|String||||holder type|mutual fund|

<a id = 'location_dim'></a>
#### location_dim

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
city|String ||||city name|Saint Paul|
state|String||||state code|MN|
country|String||||country name |United States|
zip|String|Y|||zip code|55144-1000|

<a id = 'news_dim'></a>
#### news_dim

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
uuid|String |Y|||unique Id of the news in yahoo finance|635f2ab9-e426-39a5-9d4e-0f73aef8476d|
title|String||||the title of the news|3M Advances Decarbonization Technologies, Showcases Power of Science To Address Climate Change During Climate Week NYC|
publisher|String||||The news publisher|News Direct|
link|String||||the news source url|https://finance.yahoo.com/news/3m-advances-decarbonization-technologies-showcases-184508228.html|
provider_publish_time|Integer||||date of the news publication in Epoch timestamp|1663785908|
type|String||||the type of news|STORY|

<a id = 'sector_dim'></a>
#### sector_dim

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
industry|String |Y|||industry label|Specialty Industrial Machinery|
sector|String||||sector label|Industrials|

<a id = 'tickers_news_fact'></a>
#### tickers_news_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
uuid|String ||Y|news_dim|unique identifier of the news in yahoo finance|49f562f2-9390-3d2d-b876-36690d879cd9|
ticker|String||Y|tickers_info_dim|ticker label|MMM|

<a id = 'tickers_actions_fact'></a>
#### tickers_actions_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
id|String |Y|||Id composed of the ticker label and the date of the dividends|AOS_1987-10-26|
dividends|Double||||dividend amount|0.01|
stock_splits|Double||||stock splits|0.00|
ticker|String||Y|tickers_info_dim|ticker label|AOS|

<a id = 'tickers_analysis_fact'></a>			
#### tickers_analysis_fact

Description : estimates done by financial analysts.

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
ticker|String |Y|||tickers_info_dim|MMM|
period|String|Y|||period related to the estimate|+1Y|
growth|Double||||growth amount|0.024|
earnings_estimate_avg|Double||||average earnings estimate|10.46|
earnings_estimate_low|Double ||||low earnings estimate|9.90|
earnings_estimate_high|Double||||high earnings estimate|11.31|
earnings_estimate_year_ago_eps|Double||||eps estimate one year ago|10.21|
earnings_estimate_number_of_analysts|Double||||number of analysts producing the estimates|19.00|
earnings_estimate_growth|Double||||growth estimate|0.02|
revenue_estimate_avg|Double||||average revenue estimate|339323000000.00|
revenue_estimate_low|Double||||low revenue estimate|32948000000.00|
revenue_estimate_high|Double||||high revenue estimate|35698000000.00|
eps_trend_current|Double||||current eps trend|10.46|
eps_trend_7days_ago|Double ||||eps 7 days ago|10.56|
eps_trend_30days_ago|Double||||eps 30 days ago|10.86|
eps_trend_60days_ago|Double||||eps 60 days ago|10.83|
eps_trend_90days_ago|Double||||eps 90 days ago|10.92|
eps_revisions_up_last7days|Double||||eps revisions up over the last 7 days|0.00|
eps_revisions_up_last30days|Double ||||eps revisions up over the last 30 days|0.00|
eps_revisions_down_last30days|Double||||eps revisions down over the last 30 days|4.00|
eps_revisions_down_last90days|Double||||eps revisions up over the last 90 days| None|

<a id = 'tickers_balancesheet_fact'></a>				
#### tickers_balancesheet_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
reporting_id|String |Y|||Id composed of the ticker, the report date and period|MMM_2021-12-31_year|
intangible_assets|Double||||intangible assets|5288000000.00|
capital_surplus|Double||||capital surplus|6429000000.00|
total_liab|Double||||total liabilities|31955000000.00|
total_stockholder_equity|Double ||||total stockholder equity|15046000000.00|
minority_interest|Double||||minority interest|71000000.00|
other_current_liab|Double||||other current liabilities|2090000000.00|
total_assets|Double||||total assets|47072000000.00|
common_stock|Double||||common stock|9000000.00|
other_current_assets|Double||||other current assets|229000000.00|
retained_earnings|Double||||retained earnings|45821000000.00|
other_liab|Double||||other liabilities|6180000000.00|
good_will|Double||||good will|13486000000.00|
treasury_stock|Double ||||treasury stock|None|
other_assets|Double||||other assets|2346000000.00|
cash|Double||||cash|4564000000.00|
total_current_liabilities|Double||||total current liabilities|9035000000.00|
deferred_long_term_asset_charges|Double||||deferred long term asset charges|581000000.00|
short_long_term_debt|Double ||||short long term debt|1291000000.00|
other_stockholder_equity|Double||||other stockholder equity|-6750000000.00|
property_plant_equipment|Double||||property plant equipment|10287000000.00|
total_current_assets|Double||||total current assets|15403000000.00|
long_term_investments|Double ||||long term investments|262000000.00|
net_tangible_asset|Double||||net tangible asset|201000000.00|
short_term_investments|Double||||short term investments|4770000000.00|
net_receivables|Double||||net receivables|16056000000.00|
long_term_debt|Double||||long term debt|4985000000.00|
inventory|Double ||||inventory|-3728000000.00|
deferred_long_term_liab|Double||||deferred long term liabilities|2994000000.00|
accounts_payable|Double||||accounts payable|None|
																								
<a id = 'tickers_cashflow_fact'></a>                                                                                                
#### tickers_cashflow_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
reporting_id|String |Y|||Id composed of the ticker, the report date and period|MMM_2022-06-30_quarter|
investments|Double||||investments|-154000000.00|
change_to_liabilities|Double||||change to liabilities|140000000.00|
total_cashflows_from_investing_activities|Double||||total cash flow from investing activities|-551000000.00|
net_borrowings|Double ||||net borrowings|-250000000.00|
total_cash_from_financing_activities|Double||||total cash flow from financing activities|-1048000000.00|
issuance_of_stock|Double||||issuance of stock|63000000.00|
net_income|Double||||net income|78000000.00|
change_in_cash|Double||||change in cash|-525000000.00|
repurchase_of_stock|Double||||repurchase of stock|-155000000.00|
effect_of_exchange_rate|Double||||effect of exchange rate|-53000000.00|
total_cash_from_operating_activities|Double||||total cash from operating activities|1127000000.00|
depreciation|Double||||depreciation|462000000.00|
other_cashflows_from_investing_activities|Double ||||other cash flows from investing activities|-13000000.00|
dividends_paid|Double||||paid dividends|-848000000.00|
change_to_inventory|Double||||change to inventory|-518000000.00|
change_to_account_receivables|Double||||change to account receivables|-268000000.00|
other_cashflows_from_financing_activities|Double||||other cash flows fron financing activities|-13000000.00|
change_to_net_income|Double ||||change to net income|1421000000.00|
capital_expenditures|Double||||capital expenditure|-384000000.00|
change_to_operating_activities|Double||||change to operating activities|None|
                                                                                                
														<a id = 'tickers_earnings_fact'></a>						
#### tickers_earnings_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
reporting_id|String |Y|||Id composed of the ticker and the period of the report|AOS_2Q2022_quarter|
revenue|Double||||revenue|965900000.00|
earnings|Double||||earnings|126200000.00|

<a id = 'tickers_eps_fact'></a>
#### tickers_eps_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
id|String |Y|||Id composed of the ticker and the period of the report|AOS_2021-10-28|
ticker|String||Y|tickers_info_dim|ticker label|AOS|
earnings_date|String||||earnings date|2021-10-28|
eps_estimate|Double ||||eps estimate|0.68|
reported_eps|Double||||reported eps|0.82|
surprise_ratio|Double||||surprise ratio|0.21|

<a id = 'tickers_financials_fact'></a>
#### tickers_financials_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
reporting_id|String |Y|||Id composed of the ticker, the date and period of the report|AOS_2021-09-30_quarter|
research_development|Double||||research and development|None|
effect_of_accounting_charges|Double||||effect of accounting charges|None|
income_before_tax|Double||||income before tax|None|
minority_interest|Double ||||minority interest|None|
net_income|Double||||net income|131600000.00|
selling_general_administrative|Double||||selling general administrative|None|
gross_profit|Double||||gross profit|340300000.00|
ebit|Double||||ebit|None|
operating_income|Double||||operating income|162700000.00|
other_operating_expenses|Double||||other operating expenses|1000000.00|
interest_expense|Double||||interest expense|None|
extraordinary_items|Double||||extraordinary items|None|
non_recurring|Double ||||non recurring items|None|
other_items|Double||||other items|None|
income_tax_expense|Double||||income tax expense|None|
total_revenue|Double||||total revenue|574300000.00|
total_operating_expenses|Double||||total operating expenses|None|
cost_of_revenue|Double ||||cost of revenue|None|
total_other_income_expense_net|Double||||total other income expense net amount|None|
discontinued_operations|Double||||discontinued operations|None|
net_income_from_continuing_ops|Double||||net income from continuing operations|None|
net_income_applicable_to_common_shares|Double ||||net income applicable to common shares|None|

<a id = 'tickers_historical_prices_fact'></a>
#### tickers_historical_prices_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
datetime|String |Y|||time shamp YYYY-MM-DD HH:MM:SS|2012-10-04 00:00:00|
date|String||||date of the price|2012-10-04|
ticker|String|Y|Y|tickers_info_dim|ticker label|AOS|
close|Double ||||close quote|12.534749|
high|Double||||high quote|12.584576|
low|Double||||low quote|12.380935|
open|Double||||open quote|12.471923|
volume|Double||||volume of transactions|1102400.0|
interval|Double ||||interval between quotes|1d|

<a id = 'tickers_holders_fact'></a>
#### tickers_holders_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
ticker|String |Y|Y|tickers_info_dim|ticker label|AOS|
holder|String|Y|||holder label |Proshares Tr-Proshares S&P 500 Aristocrats ETF|
shares|Double|Y|Y|tickers_info_dim|shares|2662542.00|
date_reported|String ||||date reported|2022-08-30|
out|Double||||out|0.02|
value|Double||||value|132674472|

<a id = 'tickers_info_dim'></a>
#### tickers_info_dim

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
ticker|String |Y| | |ticker label|MMM|
industry|String| | | |industry|Conglomerates|
zip|String| | | |zip|55144-1000|
currency|String| | | |currency|USD|
isin|String | | | |isin|None|
exchange|String| | | |exchange name|NYQ|
fullTimeEmployees|Integer| | | |number of full time employees|95000|
fundFamily|String| | | |family of fund|None|
fundInceptionDate|String| | | |inception date of fund|None|
isEsgPopulated|String| | | |is ESG populated|false|
lastFiscalYearEnd|Integer| | | |Last fiscal year end in Epoch timestamp|1640908800|
lastSplitDate|Integer | | | |Last split date in Epoch timestamp|1064880000|
lastSplitFactor|String| | | |factor of last split|2:1|
longBusinessSummary|String| | | |long summary of business|3M Company operates as a diversified technology company worldwide. It operates through .... The company was founded in 1902 and is based in St. Paul, Minnesota.|
longName|String| | | |company long name|3M Company|
marketCap|Double| | | |market cap|69004427264|
mostRecentQuarter|Double | | | |most recent quarter-end date in epoch timestamp|1664496000|
nextFiscalYearEnd|Double| | | |next fiscal year-end date in epoch timestamp|1703980800|
numberOfAnalystOpinions|Integer| | | |number of analyst opinions|18|
quoteType|String | | | |type of quote|EQUITY|
startDate|String| | | |start date|None|
uuid|String| | | |uuid|None|
percentage_of_shares_held_by_all_insider|Double| | | |percentage of shares held by all insiders |0.12|
percentage_of_shares_held_by_institutions|Double| | | |percentage of shares held by institutions|68.57|
percentage_of_float_held_by_institutions|Double | | | |percentage of float held by institutions|68.66|
number_of_institutions_holding_shares|Integer| | | |number of institutions holding shares|2632|

<a id = 'tickers_options_fact'></a>
#### tickers_options_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
contract_symbol|String |Y|| |option contract symbol|MMM220930P00101000|
last_trade_date|String| | | |lsat trade date|2022-09-22 13:55:55+00:00|
strike|Double| | | |strike|101.0|
last_price|Double | | | |last price|0.09|
bid|Double| | | |bid|0.06|
ask|Double| | | |ask|0.36|
change|Double | | | |change|0.09|
percent_change|Double| | | |percentage of change|NaN|
volume|Double| | | |volume|NaN|
open_interest|Integer | | | |open interest|1|
implied_volatility|Double| | | |implied volatility|0.569340244140625|
in_the_money|String| | | |in the money|false|
contract_size|String | | | |contract size|REGULAR|
currency|String| | | |currency|USD|
option_type|String| | | |option type|put|
ticker|String |Y|Y|tickers_info_dim|ticker label|MMM|

<a id = 'tickers_recommendations_fact'></a>
#### tickers_recommendations_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
firm|String |Y| | |firm issuing the recommendations|BMO|
ticker|String|Y|Y|tickers_info_dim|ticker label|AOS|
date|String|Y| | |date of recommendations|2012-06-21|
from_grade|String | | | |from grade|Market|
to_grade|String| | | |to grade|Perform|
action|String| | | |action|init|

<a id = 'exchanges_dim'></a>
#### exchanges_dim

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
exchange|String |Y| | |exchange name|JPX|
market|String| | | |market name|jp_market|

<a id = 'markets_dim'></a>
#### markets_dim

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
market|String |Y| | |market name|gb_market|
country|String| | | |country name of the market|unitedkingdom|

<a id = 'financial_report_dim'></a>
#### market_indexes_dim

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
index_id|String |Y| | |market index id|DJ:DJA|
index_name|String| | | |market index name|ow Jones Composite Average|
market|String|Y|Y|markets_dim|market name|us_market|

<a id = 'tickers_shares_fact'></a>
#### tickers_shares_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
reporting_id|String |Y| | |Id composed of the ticker the year and period type of the report|AOS_2018_year|
basic_shares|Integer| | | |volume of basic shares|168159148|

<a id = 'tickers_tweets_users_fact'></a>
#### tickers_tweets_users_fact

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
tweet_id|Integer |Y| | |tweet id|1571311640322408449|
user_id|Integer|Y| | |user id|7421502|
ticker|String|Y|Y|tickers_info_dim| |AOS|

<a id = 'tweets_dim'></a>
#### tweets_dim

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
tweet_id|Integer |Y| | |tweet id|1570849452449214464|
created_at|String| | | |creation date of tweet|2022-09-16 18:57:49|
lang|String| | | |language of tweet|en|
source|String | | | |source of tweet|Twitter for iPhone|
source_url|String| | | |url source|http://twitter.com/download/iphone|
text|String| | | |tweet text|RT @ageofsamnft: PFPs coming to soon to a theatre near you. ????\n\n@BoredApeSolClub you know what's up...\n\n#pixelart #SolanaNFTs #AOS https://t..|
in_reply_to_status_id|Integer | | | |tweet recipient id|NaN|
truncated|String| | | |is truncated|False|
							
<a id = 'user_id_dim'></a>
#### user_id_dim

field|	format|	is PK|	is FK |Foreign table| description|value example|
-|-|-|-|-|-|-|
user_id|Integer |Y|||user id|6658132|
user_created_at|String| | | |user id creation date|2007-06-08|
user_description|String| | | |Director of Partnerships @ MRI-Simmons | Past: MIT MBA, ubeMogul, SambaTV, MSFT, UC Berkeley|
user_followers_count|Integer | | | |user followers count|340|
friends_count|Integer| | | |friends count|104|
user_location|String| | | |user location|New York, NY|
user_name|String | | | |user name|Ishan Bhaumik|
user_verified|String| | | |is user verified|False|





					

					





								

								




		

		

