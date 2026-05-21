/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml.benchmarks.job.complex;

import org.apache.wayang.apps.imdb.data.*;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Arrays;
import java.util.Collection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/*
 * SELECT
 *   MIN(cn.name) AS company_name,
 *   MIN(lt.link) AS link_type,
 *   MIN(t.title) AS western_follow_up
 * FROM
 *   company_name AS cn,
 *   company_type AS ct,
 *   keyword AS k,
 *   link_type AS lt,
 *   movie_companies AS mc,
 *   movie_info AS mi,
 *   movie_keyword AS mk,
 *   movie_link AS ml,
 *   title AS t
 * WHERE
 *   cn.country_code !='[pl]'
 *   AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%')
 *   AND ct.kind ='production companies'
 *   AND k.keyword ='sequel'
 *   AND lt.link LIKE '%follow%'
 *   AND mc.note IS NULL
 *   AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark')
 *   AND t.production_year BETWEEN 1950 AND 2000
 *   AND lt.id = ml.link_type_id
 *   AND ml.movie_id = t.id
 *   AND t.id = mk.movie_id
 *   AND mk.keyword_id = k.id
 *   AND t.id = mc.movie_id
 *   AND mc.company_type_id = ct.id
 *   AND mc.company_id = cn.id
 *   AND ml.movie_id = mk.movie_id
 *   AND mk.movie_id = mc.movie_id
 *   AND ml.movie_id = mi.movie_id
 *   AND mc.movie_id = mi.movie_id
 *   AND cn.name_pcode_nf = cn.name_pcode_sf
 *   AND mi.movie_id = t.id
 *   AND ml.movie_id = mc.movie_id
 *   AND mk.movie_id = mi.movie_id;
 */
public class Query1 {

    public static WayangPlan getWayangPlan(String dataPath, Collection<LtMlTMkKMcCtCnMkMcMiMiCnTMcMi> collector){
        TextFileSource companyNameText = new TextFileSource(dataPath + "company_name.csv", "UTF-8");
        TextFileSource companyTypeText = new TextFileSource(dataPath + "company_type.csv", "UTF-8");
        TextFileSource keywordText = new TextFileSource(dataPath + "keyword.csv", "UTF-8");
        TextFileSource linkTypeText = new TextFileSource(dataPath + "link_type.csv", "UTF-8");
        TextFileSource movieCompaniesText = new TextFileSource(dataPath + "movie_companies.csv", "UTF-8");
        TextFileSource movieInfoText = new TextFileSource(dataPath + "movie_info.csv", "UTF-8");
        TextFileSource movieKeywordText = new TextFileSource(dataPath + "movie_keyword.csv", "UTF-8");
        TextFileSource movieLinkText = new TextFileSource(dataPath + "movie_link.csv", "UTF-8");
        TextFileSource titleText = new TextFileSource(dataPath + "title.csv", "UTF-8");

        MapOperator<String, CompanyName> cnParser = new MapOperator<String, CompanyName>(
            (line) -> CompanyName.parseCsv(line),
            String.class,
            CompanyName.class
        );

        MapOperator<String, CompanyType> ctParser = new MapOperator<String, CompanyType>(
            (line) -> CompanyType.parseCsv(line),
            String.class,
            CompanyType.class
        );

        MapOperator<String, Keyword> kParser = new MapOperator<String, Keyword>(
            (line) -> Keyword.parseCsv(line),
            String.class,
            Keyword.class
        );

        MapOperator<String, LinkType> ltParser = new MapOperator<String, LinkType>(
            (line) -> LinkType.parseCsv(line),
            String.class,
            LinkType.class
        );

        MapOperator<String, MovieCompanies> mcParser = new MapOperator<String, MovieCompanies>(
            (line) -> MovieCompanies.parseCsv(line),
            String.class,
            MovieCompanies.class
        );

        MapOperator<String, MovieInfo> miParser = new MapOperator<String, MovieInfo>(
            (line) -> MovieInfo.parseCsv(line),
            String.class,
            MovieInfo.class
        );

        MapOperator<String, MovieKeyword> mkParser = new MapOperator<String, MovieKeyword>(
            (line) -> MovieKeyword.parseCsv(line),
            String.class,
            MovieKeyword.class
        );

        MapOperator<String, MovieKeyword> mkParserTwo = new MapOperator<String, MovieKeyword>(mkParser);

        MapOperator<String, MovieLink> mlParser = new MapOperator<String, MovieLink>(
            (line) -> MovieLink.parseCsv(line),
            String.class,
            MovieLink.class
        );

        MapOperator<String, Title> tParser = new MapOperator<String, Title>(
            (line) -> Title.parseCsv(line),
            String.class,
            Title.class
        );

        FilterOperator<CompanyName> cnFilter = new FilterOperator<CompanyName>(
            (cn) -> cn.countryCode() != "[pl]",
            CompanyName.class
        );

        FilterOperator<CompanyName> cnFilterTwo = new FilterOperator<CompanyName>(
            (cn) -> cn.name().contains("Film") || cn.name().contains("Warner"),
            CompanyName.class
        );

        FilterOperator<CompanyName> cnFilterThree = new FilterOperator<CompanyName>(cnFilterTwo);

        FilterOperator<CompanyType> ctFilter = new FilterOperator<CompanyType>(
            (ct) -> ct.kind().equals("production companies"),
            CompanyType.class
        );

        FilterOperator<Keyword> kFilter = new FilterOperator<Keyword>(
            (k) -> k.keyword().equals("sequel"),
            Keyword.class
        );

        FilterOperator<LinkType> ltFilter = new FilterOperator<LinkType>(
            (lt) -> lt.link().contains("follow"),
            LinkType.class
        );

        FilterOperator<MovieCompanies> mcFilter = new FilterOperator<MovieCompanies>(
            (mc) -> mc.note() == null,
            MovieCompanies.class
        );

        FilterOperator<MovieCompanies> mcFilterTwo = new FilterOperator<MovieCompanies>(mcFilter);
        FilterOperator<MovieCompanies> mcFilterThree = new FilterOperator<MovieCompanies>(mcFilter);

        FilterOperator<MovieInfo> miFilter = new FilterOperator<MovieInfo>(
            (mi) -> Arrays.asList(new String[] {"Sweden", "Norway", "Germany", "Denmark"}).contains(mi.info()),
            MovieInfo.class
        );

        FilterOperator<MovieInfo> miFilterTwo = new FilterOperator<MovieInfo>(miFilter);
        FilterOperator<MovieInfo> miFilterThree = new FilterOperator<MovieInfo>(miFilter);

        FilterOperator<Title> tFilter = new FilterOperator<Title>(
            (t) -> t.productionYear() >= 1950 && t.productionYear() <= 2000,
            Title.class
        );

        FilterOperator<Title> tFilterTwo = new FilterOperator<Title>(tFilter);

        JoinOperator<LinkType, MovieLink, Integer> ltMlJoin = new JoinOperator<LinkType, MovieLink, Integer>(
            (lt) -> lt.id(),
            (ml) -> ml.id(),
            LinkType.class,
            MovieLink.class,
            Integer.class
        );

        JoinOperator<Tuple2<LinkType, MovieLink>, Title, Integer> ltMlTJoin = new JoinOperator<Tuple2<LinkType, MovieLink>, Title, Integer>(
            (ltMl) -> ltMl.field1.movieId(),
            (t) -> t.id(),
            ReflectionUtils.specify(Tuple2.class),
            Title.class,
            Integer.class
        );

        JoinOperator<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword, Integer> ltMlTMkJoin = new JoinOperator<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword, Integer>(
            (ltMlT) -> ltMlT.field1.id(),
            (mk) -> mk.movieId(),
            ReflectionUtils.specify(Tuple2.class),
            MovieKeyword.class,
            Integer.class
        );

        JoinOperator<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword, Integer> ltMlTMkKJoin = new JoinOperator<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword, Integer>(
            (ltMlTMk) -> ltMlTMk.field1.keywordId(),
            (k) -> k.id(),
            ReflectionUtils.specify(Tuple2.class),
            Keyword.class,
            Integer.class
        );

        JoinOperator<Tuple2<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword>, MovieCompanies, Integer> ltMlTMkKMcJoin = new JoinOperator<Tuple2<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword>, MovieCompanies, Integer>(
            (ltMlTMkK) -> ltMlTMkK.field0.field1.id(),
            (mc) -> mc.movieId(),
            ReflectionUtils.specify(Tuple2.class),
            MovieCompanies.class,
            Integer.class
        );


        JoinOperator<Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword>, MovieCompanies>, CompanyType, Integer> ltMlTMkKMcCtJoin = new JoinOperator<Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword>, MovieCompanies>, CompanyType, Integer>(
            (ltMlTMkKMc) -> ltMlTMkKMc.field1.companyTypeId(),
            (ct) -> ct.id(),
            ReflectionUtils.specify(Tuple2.class),
            CompanyType.class,
            Integer.class
        );


        JoinOperator<LtMlTMkKMcCt, CompanyName, Integer> ltMlTMkKMcCtCnJoin = new JoinOperator<LtMlTMkKMcCt, CompanyName, Integer>(
            (ltMlTMkKMcCt) -> ltMlTMkKMcCt.field0.field1.companyId(),
            (cn) -> cn.id(),
            ReflectionUtils.specify(LtMlTMkKMcCt.class),
            CompanyName.class,
            Integer.class
        );

        JoinOperator<Tuple2<LtMlTMkKMcCt, CompanyName>, MovieKeyword, Integer> ltMlTMkKMcCtCnMkJoin = new JoinOperator<Tuple2<LtMlTMkKMcCt, CompanyName>, MovieKeyword, Integer>(
            (ltMlTMkKMcCtCn) -> ltMlTMkKMcCtCn.field0.field0.field0.field0.field1.movieId(),
            (mk) -> mk.movieId(),
            ReflectionUtils.specify(Tuple2.class),
            MovieKeyword.class,
            Integer.class
        );

        JoinOperator<Tuple2<Tuple2<LtMlTMkKMcCt, CompanyName>, MovieKeyword>, MovieCompanies, Integer> ltMlTMkKMcCtCnMkMcJoin = new JoinOperator<Tuple2<Tuple2<LtMlTMkKMcCt, CompanyName>, MovieKeyword>, MovieCompanies, Integer>(
            (ltMlTMkKMcCtCnMk) -> ltMlTMkKMcCtCnMk.field1.movieId(),
            (mc) -> mc.movieId(),
            ReflectionUtils.specify(Tuple2.class),
            MovieCompanies.class,
            Integer.class
        );

        JoinOperator<LtMlTMkKMcCtCnMkMc, MovieInfo, Integer> ltMlTMkKMcCtCnMkMcMiJoin = new JoinOperator<LtMlTMkKMcCtCnMkMc, MovieInfo, Integer>(
            (ltMlTMkKMcCtCnMkMc) -> ltMlTMkKMcCtCnMkMc.field0.field0.field0.field0.field0.field0.field0.field0.field1.movieId(),
            (mi) -> mi.movieId(),
            ReflectionUtils.specify(LtMlTMkKMcCtCnMkMc.class),
            MovieInfo.class,
            Integer.class
        );

        JoinOperator<Tuple2<LtMlTMkKMcCtCnMkMc, MovieInfo>, MovieInfo, Integer> ltMlTMkKMcCtCnMkMcMiMiJoin = new JoinOperator<Tuple2<LtMlTMkKMcCtCnMkMc, MovieInfo>, MovieInfo, Integer>(
            (ltMlTMkKMcCtCnMkMcMi) -> ltMlTMkKMcCtCnMkMcMi.field0.field0.field0.field0.field0.field1.movieId(),
            (mi) -> mi.movieId(),
            ReflectionUtils.specify(Tuple2.class),
            MovieInfo.class,
            Integer.class
        );

        JoinOperator<Tuple2<Tuple2<LtMlTMkKMcCtCnMkMc, MovieInfo>, MovieInfo>, CompanyName, String> ltMlTMkKMcCtCnMkMcMiMiCnJoin = new JoinOperator<Tuple2<Tuple2<LtMlTMkKMcCtCnMkMc, MovieInfo>, MovieInfo>, CompanyName, String>(
            (ltMlTMkKMcCtCnMkMcMiMi) -> ltMlTMkKMcCtCnMkMcMiMi.field0.field0.field0.field0.field1.namePcodeNf(),
            (cn) -> cn.namePcodeSf(),
            ReflectionUtils.specify(Tuple2.class),
            CompanyName.class,
            String.class
        );

        JoinOperator<LtMlTMkKMcCtCnMkMcMiMiCn, Title, Integer> ltMlTMkKMcCtCnMkMcMiMiCnTJoin = new JoinOperator<LtMlTMkKMcCtCnMkMcMiMiCn, Title, Integer>(
            (ltMlTMkKMcCtCnMkMcMiMiCn) -> ltMlTMkKMcCtCnMkMcMiMiCn.field0.field1.movieId(),
            (t) -> t.id(),
            ReflectionUtils.specify(LtMlTMkKMcCtCnMkMcMiMiCn.class),
            Title.class,
            Integer.class
        );

        JoinOperator<Tuple2<LtMlTMkKMcCtCnMkMcMiMiCn, Title>, MovieCompanies, Integer> ltMlTMkKMcCtCnMkMcMiMiCnTMcJoin = new JoinOperator<Tuple2<LtMlTMkKMcCtCnMkMcMiMiCn, Title>, MovieCompanies, Integer>(
            (ltMlTMkKMcCtCnMkMcMiMiCnT) -> ltMlTMkKMcCtCnMkMcMiMiCnT.field0.field0.field0.field0.field0.field0.field0.field0.field1.movieId(),
            (mc) -> mc.movieId(),
            ReflectionUtils.specify(Tuple2.class),
            MovieCompanies.class,
            Integer.class
        );

        JoinOperator<Tuple2<Tuple2<LtMlTMkKMcCtCnMkMcMiMiCn, Title>, MovieCompanies>, MovieInfo, Integer> ltMlTMkKMcCtCnMkMcMiMiCnTMcMiJoin = new JoinOperator<Tuple2<Tuple2<LtMlTMkKMcCtCnMkMcMiMiCn, Title>, MovieCompanies>, MovieInfo, Integer>(
            (ltMlTMkKMcCtCnMkMcMiMiCnTMc) -> ltMlTMkKMcCtCnMkMcMiMiCnTMc.field0.field0.field0.field0.field0.field0.field1.movieId(),
            (mi) -> mi.movieId(),
            ReflectionUtils.specify(Tuple2.class),
            MovieInfo.class,
            Integer.class
        );


        ReduceByOperator<LtMlTMkKMcCtCnMkMcMiMiCnTMcMi, String> cnMin = new ReduceByOperator<LtMlTMkKMcCtCnMkMcMiMiCnTMcMi, String>(
            (tuple) -> tuple.field0.field0.field0.field1.name(),
            (t1, t2) -> {
                return t1.field0.field0.field0.field1.name().compareTo(t2.field0.field0.field0.field1.name()) <= 0 ? t1 : t2;
            },
            String.class,
            LtMlTMkKMcCtCnMkMcMiMiCnTMcMi.class
        );

        ReduceByOperator<LtMlTMkKMcCtCnMkMcMiMiCnTMcMi, String> ltMin = new ReduceByOperator<LtMlTMkKMcCtCnMkMcMiMiCnTMcMi, String>(
            (tuple) -> tuple.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.link(),
            (t1, t2) -> {
                return t1.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.link().compareTo(t2.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.field0.link()) <= 0 ? t1 : t2;
            },
            String.class,
            LtMlTMkKMcCtCnMkMcMiMiCnTMcMi.class
        );

        ReduceByOperator<LtMlTMkKMcCtCnMkMcMiMiCnTMcMi, String> tMin = new ReduceByOperator<LtMlTMkKMcCtCnMkMcMiMiCnTMcMi, String>(
            (tuple) -> tuple.field0.field0.field1.title(),
            (t1, t2) -> {
                return t1.field0.field0.field1.title().compareTo(t2.field0.field0.field1.title()) <= 0 ? t1 : t2;
            },
            String.class,
            LtMlTMkKMcCtCnMkMcMiMiCnTMcMi.class
        );

        LocalCallbackSink<LtMlTMkKMcCtCnMkMcMiMiCnTMcMi> sink = LocalCallbackSink.createCollectingSink(
                collector,
                DataSetType.createDefaultUnchecked(LtMlTMkKMcCtCnMkMcMiMiCnTMcMi.class)
        );

        //Connect all the operators
        companyNameText.connectTo(0, cnParser, 0);
        companyTypeText.connectTo(0, ctParser, 0);
        keywordText.connectTo(0, kParser, 0);
        linkTypeText.connectTo(0, ltParser, 0);
        movieCompaniesText.connectTo(0, mcParser, 0);
        movieInfoText.connectTo(0, miParser, 0);
        movieKeywordText.connectTo(0, mkParser, 0);
        movieLinkText.connectTo(0, mlParser, 0);
        titleText.connectTo(0, tParser, 0);

        cnParser.connectTo(0, cnFilter, 0);
        cnFilter.connectTo(0, cnFilterTwo, 0);
        kParser.connectTo(0, kFilter, 0);
        ltParser.connectTo(0, ltFilter, 0);
        mcParser.connectTo(0, mcFilter, 0);
        tParser.connectTo(0, tFilter, 0);
        miParser.connectTo(0, miFilter, 0);

        ltFilter.connectTo(0, ltMlJoin, 0);
        mlParser.connectTo(0, ltMlJoin, 1);

        ltMlJoin.connectTo(0, ltMlTJoin, 0);
        tFilter.connectTo(0, ltMlTJoin, 1);

        ltMlTJoin.connectTo(0, ltMlTMkJoin, 0);
        mkParser.connectTo(0, ltMlTMkJoin, 1);

        ltMlTMkJoin.connectTo(0, ltMlTMkKJoin, 0);
        kFilter.connectTo(0, ltMlTMkKJoin, 1);

        ltMlTMkKJoin.connectTo(0, ltMlTMkKMcJoin, 0);
        mcFilter.connectTo(0, ltMlTMkKMcJoin, 1);

        ltMlTMkKMcJoin.connectTo(0, ltMlTMkKMcCtJoin, 0);
        ctFilter.connectTo(0, ltMlTMkKMcCtJoin, 1);

        ltMlTMkKMcCtJoin.connectTo(0, ltMlTMkKMcCtCnJoin, 0);
        cnFilterTwo.connectTo(0, ltMlTMkKMcCtCnJoin, 1);

        ltMlTMkKMcCtCnJoin.connectTo(0, ltMlTMkKMcCtCnMkJoin, 0);
        mkParserTwo.connectTo(0, ltMlTMkKMcCtCnMkJoin, 1);

        ltMlTMkKMcCtCnMkJoin.connectTo(0, ltMlTMkKMcCtCnMkMcJoin, 0);
        mcFilterTwo.connectTo(0, ltMlTMkKMcCtCnMkMcJoin, 1);

        ltMlTMkKMcCtCnMkMcJoin.connectTo(0, ltMlTMkKMcCtCnMkMcMiJoin, 0);
        miFilter.connectTo(0, ltMlTMkKMcCtCnMkMcMiJoin, 1);

        ltMlTMkKMcCtCnMkMcMiJoin.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiJoin, 0);
        miFilterTwo.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiJoin, 1);

        ltMlTMkKMcCtCnMkMcMiMiJoin.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiCnJoin, 0);
        cnFilterThree.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiCnJoin, 1);

        ltMlTMkKMcCtCnMkMcMiMiCnJoin.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiCnTJoin, 0);
        tFilterTwo.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiCnTJoin, 1);

        ltMlTMkKMcCtCnMkMcMiMiCnTJoin.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiCnTMcJoin, 0);
        mcFilterThree.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiCnTMcJoin, 1);

        ltMlTMkKMcCtCnMkMcMiMiCnTMcJoin.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiCnTMcMiJoin, 0);
        miFilterThree.connectTo(0, ltMlTMkKMcCtCnMkMcMiMiCnTMcMiJoin, 1);

        ltMlTMkKMcCtCnMkMcMiMiCnTMcMiJoin.connectTo(0, cnMin, 0);
        cnMin.connectTo(0, ltMin, 0);
        ltMin.connectTo(0, tMin , 0);
        tMin.connectTo(0, sink, 0);


        return new WayangPlan(sink);
    }

    //Some intermediate types for intermediate join results
    private static class LtMlTMkKMcCt extends Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword>, MovieCompanies>, CompanyType>{}

    private static class LtMlTMkKMcCtCnMkMc extends Tuple2<Tuple2<Tuple2<LtMlTMkKMcCt, CompanyName>, MovieKeyword>, MovieCompanies>{}

    private static class LtMlTMkKMcCtCnMkMcMiMiCn extends Tuple2<Tuple2<Tuple2<LtMlTMkKMcCtCnMkMc, MovieInfo>, MovieInfo>, CompanyName>{}

    private static class LtMlTMkKMcCtCnMkMcMiMiCnTMcMi extends Tuple2<Tuple2<Tuple2<LtMlTMkKMcCtCnMkMcMiMiCn, Title>, MovieCompanies>, MovieInfo>{}

}
