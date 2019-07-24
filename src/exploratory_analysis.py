import seaborn as sns
import pandas as pd
import matplotlib.pyplot as
import pyspark.sql.functions as F

from src.loan_ETL import LoanData

def generateHeatMap(self):
    """
    Display a heatmap of good standing loan percentage as a function of annual income and loan amount of the loan
    requester.

    Requires processLoans() function to have already been run.
    """
    plt.clf()

    # function to more easily compute percent of loans in good standing for each bucket of annual income and
    # requested loan amount
    cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
    heatmap_df = self.loans_processed.where(
        (F.col('loan_amnt_rounded') != 0) & (F.col('annual_inc_rounded') != 0)
    ).groupBy('loan_amnt_rounded', 'annual_inc_rounded').agg(
        (cnt_cond(F.col('loan_status_simplified') == 'good_standing') / (
                    cnt_cond(F.col('loan_status_simplified') == 'good_standing') + cnt_cond(
                F.col('loan_status_simplified') == 'bad_standing'))).alias('percent_good_standing')
    ).select('annual_inc_rounded', 'loan_amnt_rounded', 'percent_good_standing')

    # create table to display heatmap
    table = heatmap_df.toPandas().pivot('annual_inc_rounded', 'loan_amnt_rounded', 'percent_good_standing')
    ax = sns.heatmap(table, xticklabels=True, yticklabels=True, linewidths=0.5)
    ax.set(xlabel='loan_amnt_rounded', ylabel='annual_inc_rounded',
           title="Percent Good Loan Statuses as Function of Loan Amount and Annual Income")
    return plt

def generateBoxPlot(self):
    """
    Display a boxplot comparing the distribution of debt-to-income ratio between loans in good standing and loans
    in bad standing (each boxplot contains 500 sample data points).

    Requires processLoans() function to have already been run.
    """
    return self.loans_processed.select('loan_status_simplified', 'dti').where(
        F.col('loan_status_simplified') == 'good_standing').sample(0.1, seed=50).take(500) + (
        self.loans_processed.where(F.col('loan_status_simplified') == 'bad_standing').sample(0.1, seed=50).take(500)
    )

def main():
    config = {
      'bucket': 'lending-club-warehouse',
      'raw_folder': 'raw_loan_data',
      'processed_folder': 'processed'
    }

    l = LoanData(config)
    l.readData(l.bucket, l.raw_folder)
    l.processLoans()
    heatmap = l.generateHeatMap()
    display(heatmap.show())
    boxplot = l.generateBoxPlot()
    display(boxplot)

if __name__ == "__main__":
    main()

