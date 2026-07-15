# CPI items actions

This repo uses Github actions to format CPIH data for the [Shopping Prices Comparison Tool](https://www.ons.gov.uk/economy/inflationandpriceindices/articles/shoppingpricescomparisontool/2023-05-03). The action is set to run at 7:01 every week day between the 14 and 27 of each month. It then runs the [script](R/postprocess.R). 

To prepare the metadata file from the excel sheet provided by the business area, copy the columns from the unchained sheet. Set the `AVERAGE_PRICE` column to a number (to remove £) and to 3dp. 

To prepare the unchained values, remove the metadata columns except the `ITEM_ID` column. Remove any columns before the reference year. You'll probably have to save the file with column renamed again in the date format expected by R which is `YYYY-MM-DD`.

There are two reference values in the R script, the reference month for average prices when the time series starts for items which is normally 5 years, so in 2023 the start was Jan 2018.

You can run the action to manually get the script to run and update files through the actions tab.

# How it works

The script reads the unchained file to read the latest month. It then goes to the ONS website to the [prices and indices dataset](https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes) and find the first link that contains some keywords and then downloads that file. It looks at the month in that file and compares it to the latest month from the `unchained.csv` file. 

If the month is different, it adds on that column to the unchained file. If it's January, it chains it onto December. Once the unchained file is updated, the script run through and created the chained indices, average prices, monthly growth and annual growth. 

The data download file then gets updated with the metadata, chained, average prices, monthly and annual growth. And the individual csv files are saved. 

# Manually running the action

It's likely the action won't run on time so you can trigger the action to run manually. Click on the action tab at the top, then `data` on the left, then the run workflow button. You'll have to run a new workflow rather than reuse an old one as it doesn't work for some reason. 
![image](https://github.com/henryjameslau/cpi-items-actions/assets/2945099/c441bd94-5e42-4b00-86da-635624d76e7a)

If you're doing this on mobile (and in bed) click on **All workflows** to open a dropdown, then select `data`

<img width="378" alt="image" src="https://github.com/ONSvisual/cpi-items-actions/assets/2945099/bbc6e097-06dd-45a3-b2bc-5a3fcd3061d7">

Then click on **Run workflow** and **Run workflow** again.

![image](https://github.com/ONSvisual/cpi-items-actions/assets/2945099/421e334f-00c7-417d-9e81-30320a9814d7)

