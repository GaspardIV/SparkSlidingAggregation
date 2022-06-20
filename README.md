# SparkSlidingAggregation
Implementation of minimal map reduce sliding aggregation algorithm in pyspark. 




<body>
<div class="cell markdown" data-collapsed="false" data-pycharm="{&quot;name&quot;:&quot;#%% md\n&quot;}">
<p>Implementation of minimal map reduce SLIDING AGGREGATION <a href="https://dl.acm.org/doi/10.1145/2463676.2463719" class="uri">https://dl.acm.org/doi/10.1145/2463676.2463719</a></p>
<p><a href="https://www.cse.cuhk.edu.hk/~taoyf/paper/sigmod13-mr.pdf" class="uri">https://www.cse.cuhk.edu.hk/~taoyf/paper/sigmod13-mr.pdf</a></p>
<p>Authors of algortithm: Yufei Tao, Wenqing Lin, Xiaokui Xiao</p>
</div>
<div class="cell markdown" data-collapsed="false" data-pycharm="{&quot;name&quot;:&quot;#%% md\n&quot;}">
<p>Yellow Taxi Trip Records (CSV) data from <a href="https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page" class="uri">https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page</a> for January 2021. For each record I've computed the average ride distance and the average passenger occupancy during the last 1000 rides. The algorithm is minimal and follows the one from the paper. It Uses Spark RDD API Python.</p>
</div>
</body>
