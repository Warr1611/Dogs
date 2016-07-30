import datetime
from zipline.api import sid, get_datetime, order_target_percent, set_benchmark, schedule_function, update_universe, record, get_fundamentals, fundamentals, date_rules, log, query, set_commission, set_slippage


class Dow30:
    def __init__(self):
        '''
        this is a utility class that can return the list of Dow30 stocks for a given date.
        the input date must be >= 1999-11-01.
        source: http://www.djindexes.com/mdsidx/downloads/brochure_info/Dow_Jones_Industrial_Average_Historical_Components.pdf
        '''

        raw_constituents = [
            ["1999-11-01", sid(4922), 1],  # Minnesota Mining & Manufacturing/3M Company
            ["1999-11-01", sid(2), 1],
            ["1999-11-01", sid(679), 1],
            ["1999-11-01", sid(7289), 1],  # AT&T Corporation
            ["1999-11-01", sid(698), 1],
            ["1999-11-01", sid(1267), 1],
            ["1999-11-01", sid(1335), 1],
            ["1999-11-01", sid(4283), 1],
            ["1999-11-01", sid(2119), 1],
            ["1999-11-01", sid(2482), 1],
            ["1999-11-01", sid(8347), 1],
            ["1999-11-01", sid(3149), 1],
            ["1999-11-01", sid(3246), 1],
            ["1999-11-01", sid(3735), 1],
            ["1999-11-01", sid(3496), 1],
            ["1999-11-01", sid(25090), 1],  # AlliedSignal Incorporated/Honeywell
            ["1999-11-01", sid(3951), 1],
            ["1999-11-01", sid(3766), 1],
            ["1999-11-01", sid(3971), 1],
            ["1999-11-01", sid(25006), 1],
            ["1999-11-01", sid(4151), 1],
            ["1999-11-01", sid(4707), 1],
            ["1999-11-01", sid(5029), 1],
            ["1999-11-01", sid(5061), 1],
            ["1999-11-01", sid(4954), 1],
            ["1999-11-01", sid(5938), 1],
            ["1999-11-01", sid(6653), 1],  # SBC Communications Incorporated/AT&T Incorporated
            ["1999-11-01", sid(7883), 1],
            ["1999-11-01", sid(8229), 1],
            ["1999-11-01", sid(2190), 1],
            ["2004-04-08", sid(7289), 0],
            ["2004-04-08", sid(2482), 0],
            ["2004-04-08", sid(3971), 0],
            ["2004-04-08", sid(239), 1],
            ["2004-04-08", sid(5923), 1],
            ["2004-04-08", sid(21839), 1],
            ["2008-02-19", sid(4954), 0],
            ["2008-02-19", sid(25090), 0],
            ["2008-02-19", sid(700), 1],
            ["2008-02-19", sid(23112), 1],
            ["2008-09-22", sid(239), 0],
            ["2008-09-22", sid(22802), 1],  # KRFT/MDLZ
            ["2009-06-08", sid(1335), 0],
            ["2009-06-08", sid(3246), 0],
            ["2009-06-08", sid(7041), 1],
            ["2009-06-08", sid(1900), 1],
            ["2012-09-24", sid(22802), 0],
            ["2012-09-24", sid(7792), 1],
            ["2013-09-23", sid(700), 0],
            ["2013-09-23", sid(3735), 0],
            ["2013-09-23", sid(20088), 1],
            ["2013-09-23", sid(35920), 1],
            ["2015-03-18", sid(6653), 0],
            ["2015-03-18", sid(24), 1]
        ]

        # translate the date string into a datetime.date
        self.data = []
        for equity in raw_constituents:
            self.data.append((
                datetime.datetime.strptime(equity[0], "%Y-%m-%d").date(),
                equity[1],
                equity[2]
            ))

    def get(self, dt):
        '''
        for a given datetime, return a list of the 30 dow stocks as Equity objects.
        dt must be >= 1999-11-01
        '''
        d = dt.date()
        ret = []
        for equity in self.data:
            if equity[0] > d:
                break
            if equity[2] == 1:
                ret.append(equity[1])
            elif equity[2] == 0:
                ret.remove(equity[1])
            else:
                raise Exception('unknown membership')
        return ret


def rebalance(context, data):
    if context.trade == True:
        log.info('REBALANCING...')
        context.trade = False
        dt = get_datetime()
        members = context.dow30.get(dt)

        for s in members:
            if s not in data:
                log.error("NoTradeDataAvailableEvent for %s" % s.symbol)

        for s in data:
            if s in members and s in context.fundamentals_df.columns:
                log.info('ordering %s, price = %.2f, yield = %.2f' % (s.symbol,
                                                                      data[s].price,
                                                                      100 * context.fundamentals_df[s].dividend_yield))
                order_target_percent(s, .1)
            else:
                order_target_percent(s, 0)
        log.info('\n')


def initialize(context):
    # Benchmark against the Dow Jones Industrial Average (DIA)
    set_benchmark(symbol('DIA'))

    # stop when trying to handle missing data
    set_nodata_policy(NoDataPolicy.EXCEPTION)

    # These are the default commission and slippage settings.  Change them to fit your
    # brokerage fees. These settings only matter for backtesting.  When you trade this
    # algorithm, they are moot - the brokerage and real market takes over.
    set_commission(commission.PerTrade(cost=0.03))
    set_slippage(slippage.VolumeShareSlippage(volume_limit=0.25, price_impact=0.1))

    # create an instance of the Dow30 class and set it within context
    context.dow30 = Dow30()

    # next trade year
    context.year = 0

    # set to True to trigger a rebalance
    context.trade = False

    # for tracking max leverage
    context.mx_lvrg = 0

    # check for possible trade, daily
    schedule_function(func=rebalance, date_rule=date_rules.every_day(), time_rule=time_rules.market_open(hours=1))


def before_trading_start(context, data):
    dt = get_datetime()

    # rebalance at the beginning or at the beginning of a new year
    if context.year == 0 or context.year == dt.year:
        context.trade = True
        context.year = dt.year + 1

        # get our dow 30 stocks
        members = context.dow30.get(dt)
        sids = [m.sid for m in members]

        # get fundamentals, save, and update universe
        # PROBLEM: this does not always sync up with Dow30
        # PROBLEM: example, returns "CAT"/1267 and "CAT_WI"/11740
        fundamentals_df = get_fundamentals(
            query(
                fundamentals.valuation_ratios.pe_ratio,
                fundamentals.valuation_ratios.dividend_yield,
            )
                .filter(fundamentals.share_class_reference.sid.in_(sids))
                .order_by(
                fundamentals.valuation_ratios.dividend_yield.desc()  # sort by highest to lowest dividend yield
            )
                .limit(10)  # take the top 10 highest paying dividend
        )

        context.fundamentals_df = fundamentals_df  # save to context to use during rebalance()
        update_universe(fundamentals_df.columns.values)


def handle_data(context, data):
    record(positions=len(context.portfolio.positions), leverage=context.account.leverage)

    if context.account.leverage > context.mx_lvrg:
        context.mx_lvrg = context.account.leverage
        record(mx_lvrg=context.mx_lvrg)  # Record maximum leverage encountered
