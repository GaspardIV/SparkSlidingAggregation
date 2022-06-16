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
<div class="cell code" data-autoscroll="auto" data-pycharm="{&quot;is_executing&quot;:true,&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb1"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb1-1"><a href="#cb1-1" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb1-2"><a href="#cb1-2" aria-hidden="true" tabindex="-1"></a># config</span>
<span id="cb1-3"><a href="#cb1-3" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb1-4"><a href="#cb1-4" aria-hidden="true" tabindex="-1"></a>FILE <span class="op">=</span> <span class="st">&quot;yellow_tripdata_2021-01.csv&quot;</span></span>
<span id="cb1-5"><a href="#cb1-5" aria-hidden="true" tabindex="-1"></a>COL_NAMES <span class="op">=</span> <span class="op">[</span><span class="st">&quot;tpep_dropoff_datetime&quot;</span><span class="op">,</span> <span class="st">&quot;passenger_count&quot;</span><span class="op">,</span> <span class="st">&quot;trip_distance&quot;</span><span class="op">]</span></span>
<span id="cb1-6"><a href="#cb1-6" aria-hidden="true" tabindex="-1"></a>AGGREGATE_FIELD <span class="op">=</span> <span class="st">&quot;passenger_count&quot;</span></span>
<span id="cb1-7"><a href="#cb1-7" aria-hidden="true" tabindex="-1"></a># AGGREGATE_FIELD <span class="op">=</span> <span class="st">&quot;trip_distance&quot;</span></span>
<span id="cb1-8"><a href="#cb1-8" aria-hidden="true" tabindex="-1"></a>AGGREGATE_COLUMN_NAME <span class="op">=</span> <span class="st">&quot;aggregate_result&quot;</span></span>
<span id="cb1-9"><a href="#cb1-9" aria-hidden="true" tabindex="-1"></a>SORT_FIELD <span class="op">=</span> <span class="st">&quot;tpep_dropoff_datetime&quot;</span></span>
<span id="cb1-10"><a href="#cb1-10" aria-hidden="true" tabindex="-1"></a>t <span class="op">=</span> <span class="dv">8</span>  # num of machines</span>
<span id="cb1-11"><a href="#cb1-11" aria-hidden="true" tabindex="-1"></a>l <span class="op">=</span> <span class="dv">1000</span>  # length of window</span></code></pre></div>
</div>
<div class="cell code" data-execution_count="3" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb2"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb2-1"><a href="#cb2-1" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb2-2"><a href="#cb2-2" aria-hidden="true" tabindex="-1"></a># globals</span>
<span id="cb2-3"><a href="#cb2-3" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb2-4"><a href="#cb2-4" aria-hidden="true" tabindex="-1"></a><span class="kw">import</span> random</span>
<span id="cb2-5"><a href="#cb2-5" aria-hidden="true" tabindex="-1"></a>from math <span class="kw">import</span> ceil</span>
<span id="cb2-6"><a href="#cb2-6" aria-hidden="true" tabindex="-1"></a>from math <span class="kw">import</span> log</span>
<span id="cb2-7"><a href="#cb2-7" aria-hidden="true" tabindex="-1"></a>from operator <span class="kw">import</span> add</span>
<span id="cb2-8"><a href="#cb2-8" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb2-9"><a href="#cb2-9" aria-hidden="true" tabindex="-1"></a>from pyspark<span class="op">.</span>sql <span class="kw">import</span> SparkSession</span>
<span id="cb2-10"><a href="#cb2-10" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb2-11"><a href="#cb2-11" aria-hidden="true" tabindex="-1"></a>DUMMY <span class="op">=</span> <span class="er">&#39;</span>DUMMY<span class="er">&#39;</span></span>
<span id="cb2-12"><a href="#cb2-12" aria-hidden="true" tabindex="-1"></a>SUM_FROM_OTHER_PREFIX <span class="op">=</span> <span class="er">&#39;</span>WHOLE_SUM_FROM_<span class="er">&#39;</span></span>
<span id="cb2-13"><a href="#cb2-13" aria-hidden="true" tabindex="-1"></a>OBJECTS_FROM_OTHER_PREFIX <span class="op">=</span> <span class="er">&#39;</span>OBJECTS_FROM_<span class="er">&#39;</span></span>
<span id="cb2-14"><a href="#cb2-14" aria-hidden="true" tabindex="-1"></a>RANK_SUM_PREFIX <span class="op">=</span> <span class="er">&#39;</span>BIG_PREFIX_<span class="er">&#39;</span></span>
<span id="cb2-15"><a href="#cb2-15" aria-hidden="true" tabindex="-1"></a>RANK_KEY <span class="op">=</span> <span class="er">&#39;</span>rank<span class="er">&#39;</span></span>
<span id="cb2-16"><a href="#cb2-16" aria-hidden="true" tabindex="-1"></a>spark <span class="op">=</span> SparkSession<span class="op">.</span>builder<span class="op">.</span><span class="fu">getOrCreate</span><span class="op">()</span></span>
<span id="cb2-17"><a href="#cb2-17" aria-hidden="true" tabindex="-1"></a>m <span class="op">=</span> <span class="dv">0</span></span>
<span id="cb2-18"><a href="#cb2-18" aria-hidden="true" tabindex="-1"></a>n <span class="op">=</span> <span class="dv">0</span></span>
<span id="cb2-19"><a href="#cb2-19" aria-hidden="true" tabindex="-1"></a></span></code></pre></div>
</div>
<div class="cell code" data-execution_count="4" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb3"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb3-1"><a href="#cb3-1" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-2"><a href="#cb3-2" aria-hidden="true" tabindex="-1"></a># utlis</span>
<span id="cb3-3"><a href="#cb3-3" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-4"><a href="#cb3-4" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">get_key_value</span><span class="op">(</span>x<span class="op">):</span></span>
<span id="cb3-5"><a href="#cb3-5" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> x<span class="op">[</span>SORT_FIELD<span class="op">]</span></span>
<span id="cb3-6"><a href="#cb3-6" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-7"><a href="#cb3-7" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-8"><a href="#cb3-8" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">get_agg_value</span><span class="op">(</span>x<span class="op">):</span></span>
<span id="cb3-9"><a href="#cb3-9" aria-hidden="true" tabindex="-1"></a>    <span class="cf">try</span><span class="op">:</span></span>
<span id="cb3-10"><a href="#cb3-10" aria-hidden="true" tabindex="-1"></a>        <span class="cf">return</span> <span class="dt">float</span><span class="op">(</span>x<span class="op">[</span>AGGREGATE_FIELD<span class="op">])</span></span>
<span id="cb3-11"><a href="#cb3-11" aria-hidden="true" tabindex="-1"></a>    except<span class="op">:</span></span>
<span id="cb3-12"><a href="#cb3-12" aria-hidden="true" tabindex="-1"></a>        <span class="cf">return</span> <span class="dv">0</span></span>
<span id="cb3-13"><a href="#cb3-13" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-14"><a href="#cb3-14" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-15"><a href="#cb3-15" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">is_dummy</span><span class="op">(</span>x<span class="op">):</span></span>
<span id="cb3-16"><a href="#cb3-16" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> <span class="fu">get_key_value</span><span class="op">(</span>x<span class="op">)</span> <span class="op">==</span> DUMMY</span>
<span id="cb3-17"><a href="#cb3-17" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-18"><a href="#cb3-18" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-19"><a href="#cb3-19" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">decision</span><span class="op">(</span>row<span class="op">,</span> probability<span class="op">):</span></span>
<span id="cb3-20"><a href="#cb3-20" aria-hidden="true" tabindex="-1"></a>    <span class="cf">if</span> <span class="fu">is_dummy</span><span class="op">(</span>row<span class="op">):</span></span>
<span id="cb3-21"><a href="#cb3-21" aria-hidden="true" tabindex="-1"></a>        <span class="cf">return</span> False</span>
<span id="cb3-22"><a href="#cb3-22" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> random<span class="op">.</span><span class="fu">random</span><span class="op">()</span> <span class="op">&lt;</span> probability</span>
<span id="cb3-23"><a href="#cb3-23" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-24"><a href="#cb3-24" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-25"><a href="#cb3-25" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">compute_rho</span><span class="op">(</span>m<span class="op">,</span> n<span class="op">,</span> t<span class="op">):</span></span>
<span id="cb3-26"><a href="#cb3-26" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> <span class="dv">1</span> <span class="op">/</span> m <span class="op">*</span> <span class="fu">log</span><span class="op">(</span>n <span class="op">*</span> t<span class="op">)</span></span>
<span id="cb3-27"><a href="#cb3-27" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-28"><a href="#cb3-28" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-29"><a href="#cb3-29" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">key</span><span class="op">(</span>x<span class="op">):</span></span>
<span id="cb3-30"><a href="#cb3-30" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> x<span class="op">[</span><span class="dv">0</span><span class="op">]</span></span>
<span id="cb3-31"><a href="#cb3-31" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-32"><a href="#cb3-32" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-33"><a href="#cb3-33" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">value</span><span class="op">(</span>x<span class="op">):</span></span>
<span id="cb3-34"><a href="#cb3-34" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> x<span class="op">[</span><span class="dv">1</span><span class="op">]</span></span>
<span id="cb3-35"><a href="#cb3-35" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-36"><a href="#cb3-36" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-37"><a href="#cb3-37" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">rank</span><span class="op">(</span>o<span class="op">):</span></span>
<span id="cb3-38"><a href="#cb3-38" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> o<span class="op">[</span>RANK_KEY<span class="op">]</span></span>
<span id="cb3-39"><a href="#cb3-39" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-40"><a href="#cb3-40" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-41"><a href="#cb3-41" aria-hidden="true" tabindex="-1"></a><span class="kw">class</span> RankRangeSum<span class="op">:</span></span>
<span id="cb3-42"><a href="#cb3-42" aria-hidden="true" tabindex="-1"></a>    <span class="kw">def</span> <span class="fu">__init__</span><span class="op">(</span>self<span class="op">,</span> array<span class="op">,</span> start_rank<span class="op">):</span></span>
<span id="cb3-43"><a href="#cb3-43" aria-hidden="true" tabindex="-1"></a>        self<span class="op">.</span>prefix_sum <span class="op">=</span> <span class="op">[</span><span class="dv">0</span><span class="op">]</span></span>
<span id="cb3-44"><a href="#cb3-44" aria-hidden="true" tabindex="-1"></a>        self<span class="op">.</span>start_rank <span class="op">=</span> start_rank</span>
<span id="cb3-45"><a href="#cb3-45" aria-hidden="true" tabindex="-1"></a>        <span class="cf">for</span> x in array<span class="op">:</span></span>
<span id="cb3-46"><a href="#cb3-46" aria-hidden="true" tabindex="-1"></a>            self<span class="op">.</span>prefix_sum<span class="op">.</span><span class="fu">append</span><span class="op">(</span>x <span class="op">+</span> self<span class="op">.</span>prefix_sum<span class="op">[-</span><span class="dv">1</span><span class="op">])</span></span>
<span id="cb3-47"><a href="#cb3-47" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb3-48"><a href="#cb3-48" aria-hidden="true" tabindex="-1"></a>    <span class="kw">def</span> <span class="fu">get_sum_from_rank_range</span><span class="op">(</span>self<span class="op">,</span> i<span class="op">,</span> j<span class="op">):</span></span>
<span id="cb3-49"><a href="#cb3-49" aria-hidden="true" tabindex="-1"></a>        <span class="cf">if</span> j <span class="op">+</span> <span class="dv">1</span> <span class="op">-</span> self<span class="op">.</span>start_rank <span class="op">&gt;=</span> <span class="fu">len</span><span class="op">(</span>self<span class="op">.</span>prefix_sum<span class="op">):</span></span>
<span id="cb3-50"><a href="#cb3-50" aria-hidden="true" tabindex="-1"></a>            <span class="cf">return</span> <span class="dt">float</span><span class="op">(</span>self<span class="op">.</span>prefix_sum<span class="op">[-</span><span class="dv">1</span><span class="op">]</span> <span class="op">-</span> self<span class="op">.</span>prefix_sum<span class="op">[</span><span class="fu">max</span><span class="op">(</span>i <span class="op">-</span> self<span class="op">.</span>start_rank<span class="op">,</span> <span class="dv">0</span><span class="op">)])</span></span>
<span id="cb3-51"><a href="#cb3-51" aria-hidden="true" tabindex="-1"></a>        <span class="cf">return</span> <span class="dt">float</span><span class="op">(</span>self<span class="op">.</span>prefix_sum<span class="op">[</span>j <span class="op">+</span> <span class="dv">1</span> <span class="op">-</span> self<span class="op">.</span>start_rank<span class="op">]</span> <span class="op">-</span> self<span class="op">.</span>prefix_sum<span class="op">[</span><span class="fu">max</span><span class="op">(</span>i <span class="op">-</span> self<span class="op">.</span>start_rank<span class="op">,</span> <span class="dv">0</span><span class="op">)])</span></span></code></pre></div>
</div>
<div class="cell code" data-execution_count="5" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb4"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb4-1"><a href="#cb4-1" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb4-2"><a href="#cb4-2" aria-hidden="true" tabindex="-1"></a># data prepare <span class="op">+</span> sampling <span class="cf">for</span> terasort</span>
<span id="cb4-3"><a href="#cb4-3" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb4-4"><a href="#cb4-4" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">add_dummies</span><span class="op">(</span>df<span class="op">):</span></span>
<span id="cb4-5"><a href="#cb4-5" aria-hidden="true" tabindex="-1"></a>    global n</span>
<span id="cb4-6"><a href="#cb4-6" aria-hidden="true" tabindex="-1"></a>    <span class="cf">if</span> n <span class="op">%</span> t <span class="op">!=</span> <span class="dv">0</span><span class="op">:</span></span>
<span id="cb4-7"><a href="#cb4-7" aria-hidden="true" tabindex="-1"></a>        dummies_count_to_add <span class="op">=</span> t <span class="op">-</span> n <span class="op">%</span> t</span>
<span id="cb4-8"><a href="#cb4-8" aria-hidden="true" tabindex="-1"></a>        dummies_to_add <span class="op">=</span> <span class="op">[[</span>DUMMY <span class="cf">for</span> _ in COL_NAMES<span class="op">]</span> <span class="cf">for</span> _ in <span class="fu">range</span><span class="op">(</span>dummies_count_to_add<span class="op">)]</span></span>
<span id="cb4-9"><a href="#cb4-9" aria-hidden="true" tabindex="-1"></a>        df <span class="op">=</span> df<span class="op">.</span><span class="fu">union</span><span class="op">(</span>spark<span class="op">.</span><span class="fu">createDataFrame</span><span class="op">(</span>dummies_to_add<span class="op">))</span></span>
<span id="cb4-10"><a href="#cb4-10" aria-hidden="true" tabindex="-1"></a>        n <span class="op">=</span> df<span class="op">.</span><span class="fu">count</span><span class="op">()</span></span>
<span id="cb4-11"><a href="#cb4-11" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> df</span>
<span id="cb4-12"><a href="#cb4-12" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb4-13"><a href="#cb4-13" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb4-14"><a href="#cb4-14" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">prepare_rdd</span><span class="op">():</span></span>
<span id="cb4-15"><a href="#cb4-15" aria-hidden="true" tabindex="-1"></a>    global m<span class="op">,</span> n</span>
<span id="cb4-16"><a href="#cb4-16" aria-hidden="true" tabindex="-1"></a>    df <span class="op">=</span> spark<span class="op">.</span>read<span class="op">.</span><span class="fu">options</span><span class="op">(</span>sep<span class="op">=</span><span class="st">&quot;,&quot;</span><span class="op">,</span> header<span class="op">=</span>True<span class="op">).</span><span class="fu">csv</span><span class="op">(</span>FILE<span class="op">).</span><span class="fu">select</span><span class="op">(</span>COL_NAMES<span class="op">)</span></span>
<span id="cb4-17"><a href="#cb4-17" aria-hidden="true" tabindex="-1"></a>    n <span class="op">=</span> df<span class="op">.</span><span class="fu">count</span><span class="op">()</span></span>
<span id="cb4-18"><a href="#cb4-18" aria-hidden="true" tabindex="-1"></a>    df <span class="op">=</span> <span class="fu">add_dummies</span><span class="op">(</span>df<span class="op">)</span></span>
<span id="cb4-19"><a href="#cb4-19" aria-hidden="true" tabindex="-1"></a>    rdd <span class="op">=</span> df<span class="op">.</span>rdd<span class="op">.</span><span class="fu">repartition</span><span class="op">(</span>t<span class="op">)</span></span>
<span id="cb4-20"><a href="#cb4-20" aria-hidden="true" tabindex="-1"></a>    m <span class="op">=</span> n <span class="op">/</span> t</span>
<span id="cb4-21"><a href="#cb4-21" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> rdd</span>
<span id="cb4-22"><a href="#cb4-22" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb4-23"><a href="#cb4-23" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb4-24"><a href="#cb4-24" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">sample</span><span class="op">(</span>rdd<span class="op">,</span> rho<span class="op">):</span></span>
<span id="cb4-25"><a href="#cb4-25" aria-hidden="true" tabindex="-1"></a>    res <span class="op">=</span> rdd<span class="op">.</span><span class="fu">map</span><span class="op">(</span>lambda x<span class="op">:</span> x <span class="cf">if</span> <span class="fu">decision</span><span class="op">(</span>x<span class="op">,</span> rho<span class="op">)</span> <span class="cf">else</span> <span class="bu">None</span><span class="op">).</span><span class="fu">filter</span><span class="op">(</span>lambda x<span class="op">:</span> x is not <span class="bu">None</span><span class="op">).</span><span class="fu">collect</span><span class="op">()</span></span>
<span id="cb4-26"><a href="#cb4-26" aria-hidden="true" tabindex="-1"></a>    res <span class="op">=</span> <span class="fu">sorted</span><span class="op">(</span>res<span class="op">,</span> key<span class="op">=</span>lambda x<span class="op">:</span> <span class="fu">get_key_value</span><span class="op">(</span>x<span class="op">))</span></span>
<span id="cb4-27"><a href="#cb4-27" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> res</span>
<span id="cb4-28"><a href="#cb4-28" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb4-29"><a href="#cb4-29" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb4-30"><a href="#cb4-30" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">get_boundaries_from_sample</span><span class="op">(</span>sample<span class="op">):</span></span>
<span id="cb4-31"><a href="#cb4-31" aria-hidden="true" tabindex="-1"></a>    boundaries <span class="op">=</span> <span class="op">[]</span></span>
<span id="cb4-32"><a href="#cb4-32" aria-hidden="true" tabindex="-1"></a>    <span class="cf">for</span> i in <span class="fu">range</span><span class="op">(</span><span class="dv">1</span><span class="op">,</span> t<span class="op">):</span></span>
<span id="cb4-33"><a href="#cb4-33" aria-hidden="true" tabindex="-1"></a>        index <span class="op">=</span> i <span class="op">*</span> <span class="fu">ceil</span><span class="op">(</span><span class="fu">len</span><span class="op">(</span>sample<span class="op">)</span> <span class="op">/</span> t<span class="op">)</span> <span class="op">-</span> <span class="dv">1</span></span>
<span id="cb4-34"><a href="#cb4-34" aria-hidden="true" tabindex="-1"></a>        boundaries<span class="op">.</span><span class="fu">append</span><span class="op">(</span>sample<span class="op">[</span>index<span class="op">])</span></span>
<span id="cb4-35"><a href="#cb4-35" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb4-36"><a href="#cb4-36" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> boundaries</span></code></pre></div>
</div>
<div class="cell code" data-execution_count="6" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb5"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb5-1"><a href="#cb5-1" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb5-2"><a href="#cb5-2" aria-hidden="true" tabindex="-1"></a># tera sort</span>
<span id="cb5-3"><a href="#cb5-3" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb5-4"><a href="#cb5-4" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">tera_sort</span><span class="op">(</span>rdd<span class="op">):</span></span>
<span id="cb5-5"><a href="#cb5-5" aria-hidden="true" tabindex="-1"></a>    rho <span class="op">=</span> <span class="fu">compute_rho</span><span class="op">(</span>m<span class="op">,</span> n<span class="op">,</span> t<span class="op">)</span></span>
<span id="cb5-6"><a href="#cb5-6" aria-hidden="true" tabindex="-1"></a>    boundaries <span class="op">=</span> <span class="fu">get_boundaries_from_sample</span><span class="op">(</span><span class="fu">sample</span><span class="op">(</span>rdd<span class="op">,</span> rho<span class="op">))</span></span>
<span id="cb5-7"><a href="#cb5-7" aria-hidden="true" tabindex="-1"></a>    # In the paper it<span class="er">&#39;</span>s sent to machines directly like <span class="er">&#39;</span>RANK_SUM_PREFIX<span class="er">&#39;</span> in rank</span>
<span id="cb5-8"><a href="#cb5-8" aria-hidden="true" tabindex="-1"></a>    boundaries <span class="op">=</span> spark<span class="op">.</span>sparkContext<span class="op">.</span><span class="fu">broadcast</span><span class="op">(</span>boundaries<span class="op">)</span></span>
<span id="cb5-9"><a href="#cb5-9" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb5-10"><a href="#cb5-10" aria-hidden="true" tabindex="-1"></a>    <span class="kw">def</span> <span class="fu">map_boundaries</span><span class="op">(</span>x<span class="op">):</span></span>
<span id="cb5-11"><a href="#cb5-11" aria-hidden="true" tabindex="-1"></a>        <span class="cf">for</span> idx<span class="op">,</span> boundary in <span class="fu">enumerate</span><span class="op">(</span>boundaries<span class="op">.</span>value<span class="op">):</span></span>
<span id="cb5-12"><a href="#cb5-12" aria-hidden="true" tabindex="-1"></a>            <span class="cf">if</span> <span class="fu">get_key_value</span><span class="op">(</span>x<span class="op">)</span> <span class="op">&lt;=</span> <span class="fu">get_key_value</span><span class="op">(</span>boundary<span class="op">):</span></span>
<span id="cb5-13"><a href="#cb5-13" aria-hidden="true" tabindex="-1"></a>                <span class="cf">return</span> idx<span class="op">,</span> <span class="op">[</span>x<span class="op">]</span></span>
<span id="cb5-14"><a href="#cb5-14" aria-hidden="true" tabindex="-1"></a>        <span class="cf">return</span> <span class="fu">len</span><span class="op">(</span>boundaries<span class="op">.</span>value<span class="op">),</span> <span class="op">[</span>x<span class="op">]</span></span>
<span id="cb5-15"><a href="#cb5-15" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb5-16"><a href="#cb5-16" aria-hidden="true" tabindex="-1"></a>    res <span class="op">=</span> rdd<span class="op">.</span><span class="fu">map</span><span class="op">(</span>map_boundaries<span class="op">).</span><span class="fu">reduceByKey</span><span class="op">(</span>add<span class="op">).</span><span class="fu">map</span><span class="op">(</span>lambda x<span class="op">:</span> <span class="op">(</span><span class="fu">key</span><span class="op">(</span>x<span class="op">),</span> <span class="fu">sorted</span><span class="op">(</span><span class="fu">value</span><span class="op">(</span>x<span class="op">),</span> key<span class="op">=</span>get_key_value<span class="op">)))</span></span>
<span id="cb5-17"><a href="#cb5-17" aria-hidden="true" tabindex="-1"></a>    boundaries<span class="op">.</span><span class="fu">unpersist</span><span class="op">()</span></span>
<span id="cb5-18"><a href="#cb5-18" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> res</span></code></pre></div>
</div>
<div class="cell code" data-execution_count="7" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb6"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb6-1"><a href="#cb6-1" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb6-2"><a href="#cb6-2" aria-hidden="true" tabindex="-1"></a># ranking and perfect balance</span>
<span id="cb6-3"><a href="#cb6-3" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb6-4"><a href="#cb6-4" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">do_rank</span><span class="op">(</span>sorted_rdd<span class="op">):</span></span>
<span id="cb6-5"><a href="#cb6-5" aria-hidden="true" tabindex="-1"></a>    <span class="kw">def</span> <span class="fu">mapper</span><span class="op">(</span>v<span class="op">):</span></span>
<span id="cb6-6"><a href="#cb6-6" aria-hidden="true" tabindex="-1"></a>        <span class="cf">return</span> <span class="op">[</span>v<span class="op">]</span> <span class="op">+</span> <span class="op">[(</span>receiver<span class="op">,</span> <span class="op">[(</span>RANK_SUM_PREFIX <span class="op">+</span> <span class="fu">str</span><span class="op">(</span><span class="fu">key</span><span class="op">(</span>v<span class="op">)),</span> <span class="fu">len</span><span class="op">(</span><span class="fu">value</span><span class="op">(</span>v<span class="op">)))])</span> <span class="cf">for</span> receiver in</span>
<span id="cb6-7"><a href="#cb6-7" aria-hidden="true" tabindex="-1"></a>                      <span class="fu">range</span><span class="op">(</span><span class="fu">key</span><span class="op">(</span>v<span class="op">)</span> <span class="op">+</span> <span class="dv">1</span><span class="op">,</span> t<span class="op">)]</span></span>
<span id="cb6-8"><a href="#cb6-8" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb6-9"><a href="#cb6-9" aria-hidden="true" tabindex="-1"></a>    <span class="kw">def</span> <span class="fu">compute_rank</span><span class="op">(</span>x<span class="op">):</span></span>
<span id="cb6-10"><a href="#cb6-10" aria-hidden="true" tabindex="-1"></a>        big_prefixes_sum <span class="op">=</span> <span class="fu">sum</span><span class="op">(</span><span class="fu">map</span><span class="op">(</span>lambda prefix_tuple<span class="op">:</span> <span class="fu">value</span><span class="op">(</span>prefix_tuple<span class="op">),</span></span>
<span id="cb6-11"><a href="#cb6-11" aria-hidden="true" tabindex="-1"></a>                                   <span class="fu">filter</span><span class="op">(</span>lambda el<span class="op">:</span> <span class="fu">key</span><span class="op">(</span>el<span class="op">).</span><span class="fu">startswith</span><span class="op">(</span>RANK_SUM_PREFIX<span class="op">),</span> <span class="fu">value</span><span class="op">(</span>x<span class="op">))))</span></span>
<span id="cb6-12"><a href="#cb6-12" aria-hidden="true" tabindex="-1"></a>        elements <span class="op">=</span> <span class="fu">list</span><span class="op">(</span><span class="fu">filter</span><span class="op">(</span>lambda el<span class="op">:</span> not <span class="fu">key</span><span class="op">(</span>el<span class="op">).</span><span class="fu">startswith</span><span class="op">(</span>RANK_SUM_PREFIX<span class="op">),</span> <span class="fu">value</span><span class="op">(</span>x<span class="op">)))</span></span>
<span id="cb6-13"><a href="#cb6-13" aria-hidden="true" tabindex="-1"></a>        <span class="cf">for</span> idx<span class="op">,</span> el in <span class="fu">enumerate</span><span class="op">(</span>elements<span class="op">):</span></span>
<span id="cb6-14"><a href="#cb6-14" aria-hidden="true" tabindex="-1"></a>            elements<span class="op">[</span>idx<span class="op">]</span> <span class="op">=</span> el<span class="op">.</span><span class="fu">asDict</span><span class="op">()</span></span>
<span id="cb6-15"><a href="#cb6-15" aria-hidden="true" tabindex="-1"></a>            elements<span class="op">[</span>idx<span class="op">][</span>RANK_KEY<span class="op">]</span> <span class="op">=</span> idx <span class="op">+</span> big_prefixes_sum <span class="op">+</span> <span class="dv">1</span></span>
<span id="cb6-16"><a href="#cb6-16" aria-hidden="true" tabindex="-1"></a>        <span class="cf">return</span> x<span class="op">[</span><span class="dv">0</span><span class="op">],</span> elements</span>
<span id="cb6-17"><a href="#cb6-17" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb6-18"><a href="#cb6-18" aria-hidden="true" tabindex="-1"></a>    res <span class="op">=</span> sorted_rdd<span class="op">.</span><span class="fu">flatMap</span><span class="op">(</span>mapper<span class="op">).</span><span class="fu">reduceByKey</span><span class="op">(</span>add<span class="op">).</span><span class="fu">map</span><span class="op">(</span>compute_rank<span class="op">)</span></span>
<span id="cb6-19"><a href="#cb6-19" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> res</span>
<span id="cb6-20"><a href="#cb6-20" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb6-21"><a href="#cb6-21" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb6-22"><a href="#cb6-22" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">perfect_balance</span><span class="op">(</span>ranked_rdd<span class="op">):</span></span>
<span id="cb6-23"><a href="#cb6-23" aria-hidden="true" tabindex="-1"></a>    res <span class="op">=</span> ranked_rdd<span class="op">.</span><span class="fu">flatMap</span><span class="op">(</span>lambda x<span class="op">:</span> <span class="fu">value</span><span class="op">(</span>x<span class="op">)).</span><span class="fu">map</span><span class="op">(</span>lambda x<span class="op">:</span> <span class="op">(</span><span class="fu">ceil</span><span class="op">(</span><span class="fu">rank</span><span class="op">(</span>x<span class="op">)</span> <span class="op">/</span> m <span class="op">-</span> <span class="dv">1</span><span class="op">),</span> <span class="op">[</span>x<span class="op">])).</span><span class="fu">reduceByKey</span><span class="op">(</span>add<span class="op">).</span><span class="fu">map</span><span class="op">(</span></span>
<span id="cb6-24"><a href="#cb6-24" aria-hidden="true" tabindex="-1"></a>        lambda x<span class="op">:</span> <span class="op">(</span><span class="dt">int</span><span class="op">(</span><span class="fu">key</span><span class="op">(</span>x<span class="op">)),</span> <span class="fu">sorted</span><span class="op">(</span><span class="fu">value</span><span class="op">(</span>x<span class="op">),</span> key<span class="op">=</span>get_key_value<span class="op">)))</span></span>
<span id="cb6-25"><a href="#cb6-25" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> res</span></code></pre></div>
</div>
<div class="cell code" data-execution_count="8" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb7"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb7-1"><a href="#cb7-1" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-2"><a href="#cb7-2" aria-hidden="true" tabindex="-1"></a># sliding aggregation</span>
<span id="cb7-3"><a href="#cb7-3" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-4"><a href="#cb7-4" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">sliding_agregation</span><span class="op">(</span>balanced_rdd<span class="op">):</span></span>
<span id="cb7-5"><a href="#cb7-5" aria-hidden="true" tabindex="-1"></a>    <span class="kw">def</span> <span class="fu">send_objects_to_other_machines</span><span class="op">(</span>v<span class="op">):</span></span>
<span id="cb7-6"><a href="#cb7-6" aria-hidden="true" tabindex="-1"></a>        whole_sums <span class="op">=</span> <span class="op">[(</span>i<span class="op">,</span> <span class="op">[(</span>SUM_FROM_OTHER_PREFIX <span class="op">+</span> <span class="fu">str</span><span class="op">(</span>v<span class="op">[</span><span class="dv">0</span><span class="op">]),</span> <span class="fu">sum</span><span class="op">(</span><span class="fu">map</span><span class="op">(</span>get_agg_value<span class="op">,</span> v<span class="op">[</span><span class="dv">1</span><span class="op">])))])</span> <span class="cf">for</span> i in <span class="fu">range</span><span class="op">(</span>v<span class="op">[</span><span class="dv">0</span><span class="op">]+</span><span class="dv">1</span><span class="op">,</span> t<span class="op">)]</span></span>
<span id="cb7-7"><a href="#cb7-7" aria-hidden="true" tabindex="-1"></a>        objects_to_send <span class="op">=</span> <span class="op">[]</span></span>
<span id="cb7-8"><a href="#cb7-8" aria-hidden="true" tabindex="-1"></a>        <span class="cf">if</span> l <span class="op">&lt;=</span> m<span class="op">:</span></span>
<span id="cb7-9"><a href="#cb7-9" aria-hidden="true" tabindex="-1"></a>            <span class="cf">if</span> <span class="fu">key</span><span class="op">(</span>v<span class="op">)</span> <span class="op">+</span> <span class="dv">1</span> <span class="op">&lt;</span> t<span class="op">:</span></span>
<span id="cb7-10"><a href="#cb7-10" aria-hidden="true" tabindex="-1"></a>                objects_to_send <span class="op">=</span> <span class="op">[(</span><span class="fu">key</span><span class="op">(</span>v<span class="op">)</span> <span class="op">+</span> <span class="dv">1</span><span class="op">,</span> <span class="op">[(</span>OBJECTS_FROM_OTHER_PREFIX <span class="op">+</span> <span class="fu">str</span><span class="op">(</span><span class="fu">key</span><span class="op">(</span>v<span class="op">)),</span> <span class="fu">value</span><span class="op">(</span>v<span class="op">))])]</span></span>
<span id="cb7-11"><a href="#cb7-11" aria-hidden="true" tabindex="-1"></a>        <span class="cf">else</span><span class="op">:</span></span>
<span id="cb7-12"><a href="#cb7-12" aria-hidden="true" tabindex="-1"></a>            receiver1 <span class="op">=</span> <span class="dt">int</span><span class="op">(</span><span class="fu">key</span><span class="op">(</span>v<span class="op">)</span> <span class="op">+</span> <span class="op">(</span>l <span class="op">-</span> <span class="dv">1</span><span class="op">)</span> <span class="co">// m)</span></span>
<span id="cb7-13"><a href="#cb7-13" aria-hidden="true" tabindex="-1"></a>            receiver2 <span class="op">=</span> receiver1 <span class="op">+</span> <span class="dv">1</span></span>
<span id="cb7-14"><a href="#cb7-14" aria-hidden="true" tabindex="-1"></a>            <span class="cf">if</span> receiver1 <span class="op">&lt;</span> t<span class="op">:</span></span>
<span id="cb7-15"><a href="#cb7-15" aria-hidden="true" tabindex="-1"></a>                objects_to_send <span class="op">=</span> objects_to_send <span class="op">+</span> <span class="op">[(</span>receiver1<span class="op">,</span> <span class="op">[(</span>OBJECTS_FROM_OTHER_PREFIX <span class="op">+</span> <span class="fu">str</span><span class="op">(</span><span class="fu">key</span><span class="op">(</span>v<span class="op">)),</span> <span class="fu">value</span><span class="op">(</span>v<span class="op">))])]</span></span>
<span id="cb7-16"><a href="#cb7-16" aria-hidden="true" tabindex="-1"></a>            <span class="cf">if</span> receiver2 <span class="op">&lt;</span> t<span class="op">:</span></span>
<span id="cb7-17"><a href="#cb7-17" aria-hidden="true" tabindex="-1"></a>                objects_to_send <span class="op">=</span> objects_to_send <span class="op">+</span> <span class="op">[(</span>receiver2<span class="op">,</span> <span class="op">[(</span>OBJECTS_FROM_OTHER_PREFIX <span class="op">+</span> <span class="fu">str</span><span class="op">(</span><span class="fu">key</span><span class="op">(</span>v<span class="op">)),</span> <span class="fu">value</span><span class="op">(</span>v<span class="op">))])]</span></span>
<span id="cb7-18"><a href="#cb7-18" aria-hidden="true" tabindex="-1"></a>        <span class="cf">return</span> objects_to_send <span class="op">+</span> whole_sums <span class="op">+</span> <span class="op">[</span>v<span class="op">]</span></span>
<span id="cb7-19"><a href="#cb7-19" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-20"><a href="#cb7-20" aria-hidden="true" tabindex="-1"></a>    <span class="kw">def</span> <span class="fu">compute_windows</span><span class="op">(</span>x<span class="op">):</span></span>
<span id="cb7-21"><a href="#cb7-21" aria-hidden="true" tabindex="-1"></a>        <span class="op">(</span>i<span class="op">,</span> value<span class="op">)</span> <span class="op">=</span> x</span>
<span id="cb7-22"><a href="#cb7-22" aria-hidden="true" tabindex="-1"></a>        own_objects <span class="op">=</span> <span class="fu">list</span><span class="op">(</span><span class="fu">filter</span><span class="op">(</span>lambda el<span class="op">:</span> <span class="kw">type</span><span class="op">(</span>el<span class="op">)</span> is dict<span class="op">,</span> value<span class="op">))</span></span>
<span id="cb7-23"><a href="#cb7-23" aria-hidden="true" tabindex="-1"></a>        without_own_objects <span class="op">=</span> <span class="fu">list</span><span class="op">(</span><span class="fu">filter</span><span class="op">(</span>lambda els<span class="op">:</span> <span class="kw">type</span><span class="op">(</span>els<span class="op">)</span> is not dict<span class="op">,</span> value<span class="op">))</span></span>
<span id="cb7-24"><a href="#cb7-24" aria-hidden="true" tabindex="-1"></a>        objects_from_others <span class="op">=</span> <span class="fu">dict</span><span class="op">(</span><span class="fu">map</span><span class="op">(</span>lambda els<span class="op">:</span> <span class="op">(</span><span class="dt">int</span><span class="op">(</span>els<span class="op">[</span><span class="dv">0</span><span class="op">][</span><span class="fu">len</span><span class="op">(</span>OBJECTS_FROM_OTHER_PREFIX<span class="op">):]),</span> els<span class="op">[</span><span class="dv">1</span><span class="op">]),</span></span>
<span id="cb7-25"><a href="#cb7-25" aria-hidden="true" tabindex="-1"></a>                                       <span class="fu">filter</span><span class="op">(</span>lambda els<span class="op">:</span> els<span class="op">[</span><span class="dv">0</span><span class="op">].</span><span class="fu">startswith</span><span class="op">(</span>OBJECTS_FROM_OTHER_PREFIX<span class="op">),</span></span>
<span id="cb7-26"><a href="#cb7-26" aria-hidden="true" tabindex="-1"></a>                                              without_own_objects<span class="op">)))</span></span>
<span id="cb7-27"><a href="#cb7-27" aria-hidden="true" tabindex="-1"></a>        whole_sums <span class="op">=</span> <span class="fu">dict</span><span class="op">(</span><span class="fu">map</span><span class="op">(</span>lambda els<span class="op">:</span> <span class="op">(</span><span class="dt">int</span><span class="op">(</span>els<span class="op">[</span><span class="dv">0</span><span class="op">][</span><span class="fu">len</span><span class="op">(</span>SUM_FROM_OTHER_PREFIX<span class="op">):]),</span> els<span class="op">[</span><span class="dv">1</span><span class="op">]),</span></span>
<span id="cb7-28"><a href="#cb7-28" aria-hidden="true" tabindex="-1"></a>                              <span class="fu">filter</span><span class="op">(</span>lambda els<span class="op">:</span> els<span class="op">[</span><span class="dv">0</span><span class="op">].</span><span class="fu">startswith</span><span class="op">(</span>SUM_FROM_OTHER_PREFIX<span class="op">),</span> without_own_objects<span class="op">)))</span></span>
<span id="cb7-29"><a href="#cb7-29" aria-hidden="true" tabindex="-1"></a>        prefix_sum_own_objects <span class="op">=</span> <span class="fu">RankRangeSum</span><span class="op">(</span><span class="fu">map</span><span class="op">(</span>get_agg_value<span class="op">,</span> own_objects<span class="op">),</span> <span class="fu">rank</span><span class="op">(</span>own_objects<span class="op">[</span><span class="dv">0</span><span class="op">]))</span></span>
<span id="cb7-30"><a href="#cb7-30" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-31"><a href="#cb7-31" aria-hidden="true" tabindex="-1"></a>        prefix_sums_of_others_objects <span class="op">=</span> <span class="fu">dict</span><span class="op">(</span></span>
<span id="cb7-32"><a href="#cb7-32" aria-hidden="true" tabindex="-1"></a>            <span class="fu">map</span><span class="op">(</span>lambda item<span class="op">:</span> <span class="op">(</span>item<span class="op">[</span><span class="dv">0</span><span class="op">],</span> <span class="fu">RankRangeSum</span><span class="op">(</span><span class="fu">map</span><span class="op">(</span>get_agg_value<span class="op">,</span> item<span class="op">[</span><span class="dv">1</span><span class="op">]),</span> <span class="fu">rank</span><span class="op">(</span>item<span class="op">[</span><span class="dv">1</span><span class="op">][</span><span class="dv">0</span><span class="op">]))),</span></span>
<span id="cb7-33"><a href="#cb7-33" aria-hidden="true" tabindex="-1"></a>                objects_from_others<span class="op">.</span><span class="fu">items</span><span class="op">()))</span></span>
<span id="cb7-34"><a href="#cb7-34" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-35"><a href="#cb7-35" aria-hidden="true" tabindex="-1"></a>        <span class="cf">for</span> o in own_objects<span class="op">:</span></span>
<span id="cb7-36"><a href="#cb7-36" aria-hidden="true" tabindex="-1"></a>            window_o <span class="op">=</span> <span class="dv">0</span></span>
<span id="cb7-37"><a href="#cb7-37" aria-hidden="true" tabindex="-1"></a>            to_rank <span class="op">=</span> <span class="fu">rank</span><span class="op">(</span>o<span class="op">)</span></span>
<span id="cb7-38"><a href="#cb7-38" aria-hidden="true" tabindex="-1"></a>            from_rank <span class="op">=</span> to_rank <span class="op">-</span> l <span class="op">+</span> <span class="dv">1</span></span>
<span id="cb7-39"><a href="#cb7-39" aria-hidden="true" tabindex="-1"></a>            alpha <span class="op">=</span> <span class="fu">ceil</span><span class="op">(</span>from_rank <span class="op">/</span> m<span class="op">)</span> <span class="op">-</span> <span class="dv">1</span></span>
<span id="cb7-40"><a href="#cb7-40" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-41"><a href="#cb7-41" aria-hidden="true" tabindex="-1"></a>            <span class="cf">if</span> alpha <span class="op">&gt;=</span> <span class="dv">0</span> and alpha <span class="op">!=</span> i<span class="op">:</span></span>
<span id="cb7-42"><a href="#cb7-42" aria-hidden="true" tabindex="-1"></a>                window_o <span class="op">+=</span> prefix_sums_of_others_objects<span class="op">[</span>alpha<span class="op">].</span><span class="fu">get_sum_from_rank_range</span><span class="op">(</span>from_rank<span class="op">,</span> to_rank<span class="op">)</span></span>
<span id="cb7-43"><a href="#cb7-43" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-44"><a href="#cb7-44" aria-hidden="true" tabindex="-1"></a>            <span class="cf">if</span> alpha <span class="op">!=</span> i<span class="op">:</span></span>
<span id="cb7-45"><a href="#cb7-45" aria-hidden="true" tabindex="-1"></a>                window_o <span class="op">+=</span> <span class="fu">sum</span><span class="op">(</span><span class="fu">map</span><span class="op">(</span>lambda x<span class="op">:</span> x<span class="op">[</span><span class="dv">1</span><span class="op">],</span> <span class="fu">filter</span><span class="op">(</span>lambda x<span class="op">:</span> alpha <span class="op">&lt;</span> x<span class="op">[</span><span class="dv">0</span><span class="op">]</span> <span class="op">&lt;</span> i<span class="op">,</span> whole_sums<span class="op">.</span><span class="fu">items</span><span class="op">())))</span></span>
<span id="cb7-46"><a href="#cb7-46" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-47"><a href="#cb7-47" aria-hidden="true" tabindex="-1"></a>            window_o <span class="op">+=</span> prefix_sum_own_objects<span class="op">.</span><span class="fu">get_sum_from_rank_range</span><span class="op">(</span>from_rank<span class="op">,</span> to_rank<span class="op">)</span></span>
<span id="cb7-48"><a href="#cb7-48" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-49"><a href="#cb7-49" aria-hidden="true" tabindex="-1"></a>            o<span class="op">[</span>AGGREGATE_COLUMN_NAME<span class="op">]</span> <span class="op">=</span> window_o <span class="op">/</span> <span class="fu">min</span><span class="op">(</span><span class="fu">rank</span><span class="op">(</span>o<span class="op">),</span> l<span class="op">)</span></span>
<span id="cb7-50"><a href="#cb7-50" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-51"><a href="#cb7-51" aria-hidden="true" tabindex="-1"></a>        own_objects <span class="op">=</span> <span class="fu">list</span><span class="op">(</span><span class="fu">filter</span><span class="op">(</span>lambda x<span class="op">:</span> not <span class="fu">is_dummy</span><span class="op">(</span>x<span class="op">),</span> own_objects<span class="op">))</span></span>
<span id="cb7-52"><a href="#cb7-52" aria-hidden="true" tabindex="-1"></a>        <span class="cf">return</span> own_objects</span>
<span id="cb7-53"><a href="#cb7-53" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb7-54"><a href="#cb7-54" aria-hidden="true" tabindex="-1"></a>    res <span class="op">=</span> balanced_rdd<span class="op">.</span><span class="fu">flatMap</span><span class="op">(</span>send_objects_to_other_machines<span class="op">).</span><span class="fu">reduceByKey</span><span class="op">(</span>add<span class="op">).</span><span class="fu">flatMap</span><span class="op">(</span>compute_windows<span class="op">)</span></span>
<span id="cb7-55"><a href="#cb7-55" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> res</span></code></pre></div>
</div>
<div class="cell code" data-execution_count="9" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb8"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb8-1"><a href="#cb8-1" aria-hidden="true" tabindex="-1"></a><span class="op">%</span>spark<span class="op">.</span>pyspark</span>
<span id="cb8-2"><a href="#cb8-2" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb8-3"><a href="#cb8-3" aria-hidden="true" tabindex="-1"></a># main</span>
<span id="cb8-4"><a href="#cb8-4" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb8-5"><a href="#cb8-5" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> <span class="fu">execute_sliding_window</span><span class="op">():</span></span>
<span id="cb8-6"><a href="#cb8-6" aria-hidden="true" tabindex="-1"></a>    rdd <span class="op">=</span> <span class="fu">prepare_rdd</span><span class="op">()</span></span>
<span id="cb8-7"><a href="#cb8-7" aria-hidden="true" tabindex="-1"></a>    sorted_rdd <span class="op">=</span> <span class="fu">tera_sort</span><span class="op">(</span>rdd<span class="op">)</span></span>
<span id="cb8-8"><a href="#cb8-8" aria-hidden="true" tabindex="-1"></a>    ranked_rdd <span class="op">=</span> <span class="fu">do_rank</span><span class="op">(</span>sorted_rdd<span class="op">)</span></span>
<span id="cb8-9"><a href="#cb8-9" aria-hidden="true" tabindex="-1"></a>    balanced_rdd <span class="op">=</span> <span class="fu">perfect_balance</span><span class="op">(</span>ranked_rdd<span class="op">)</span></span>
<span id="cb8-10"><a href="#cb8-10" aria-hidden="true" tabindex="-1"></a>    aggregated_rdd <span class="op">=</span> <span class="fu">sliding_agregation</span><span class="op">(</span>balanced_rdd<span class="op">)</span></span>
<span id="cb8-11"><a href="#cb8-11" aria-hidden="true" tabindex="-1"></a>    result <span class="op">=</span> aggregated_rdd<span class="op">.</span><span class="fu">collect</span><span class="op">()</span></span>
<span id="cb8-12"><a href="#cb8-12" aria-hidden="true" tabindex="-1"></a>    df <span class="op">=</span> spark<span class="op">.</span><span class="fu">createDataFrame</span><span class="op">(</span>result<span class="op">)</span></span>
<span id="cb8-13"><a href="#cb8-13" aria-hidden="true" tabindex="-1"></a>    df<span class="op">.</span><span class="fu">show</span><span class="op">(</span><span class="dv">1200</span><span class="op">)</span></span></code></pre></div>
</div>
<div class="cell code" data-execution_count="10" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb9"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb9-1"><a href="#cb9-1" aria-hidden="true" tabindex="-1"></a><span class="fu">execute_sliding_window</span><span class="op">()</span></span></code></pre></div>
</div>
<div class="cell code" data-execution_count="11" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb10"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb10-1"><a href="#cb10-1" aria-hidden="true" tabindex="-1"></a>AGGREGATE_FIELD <span class="op">=</span> <span class="st">&quot;trip_distance&quot;</span></span>
<span id="cb10-2"><a href="#cb10-2" aria-hidden="true" tabindex="-1"></a><span class="fu">execute_sliding_window</span><span class="op">()</span></span></code></pre></div>
</div>
<div class="cell code" data-execution_count="12" data-autoscroll="auto" data-pycharm="{&quot;name&quot;:&quot;#%%\n&quot;}">
<div class="sourceCode" id="cb11"><pre class="sourceCode scala"><code class="sourceCode scala"><span id="cb11-1"><a href="#cb11-1" aria-hidden="true" tabindex="-1"></a></span></code></pre></div>
</div>
</body>
