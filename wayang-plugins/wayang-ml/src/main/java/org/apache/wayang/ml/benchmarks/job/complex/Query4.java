/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.wayang.ml.benchmarks.job.complex;

import org.apache.wayang.apps.imdb.data.*;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;

import java.util.Arrays;
import java.util.Collection;

/**
 * Query4:
 *
 * SELECT MIN(cn.name) AS company_name,
 *        MIN(lt.link) AS link_type,
 *        MIN(t.title) AS western_follow_up
 * FROM company_name AS cn,
 *      company_type AS ct,
 *      keyword AS k,
 *      link_type AS lt,
 *      movie_companies AS mc,
 *      movie_info AS mi,
 *      movie_keyword AS mk,
 *      movie_link AS ml,
 *      title AS t
 * WHERE cn.country_code !='[pl]'
 *   AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%')
 *   AND ct.kind ='production companies'
 *   AND k.keyword ='sequel'
 *   AND lt.link LIKE '%follow%'
 *   AND mc.note IS NULL
 *   AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark',
 *                   'Swedish', 'Denish', 'Norwegian', 'German')
 *   AND t.production_year BETWEEN 1950 AND 2000
 *   AND lt.id = ml.link_type_id
 *   AND ml.movie_id = t.id
 *   AND t.id = mk.movie_id
 *   AND mk.keyword_id = k.id
 *   AND t.id = mc.movie_id
 *   AND mc.company_type_id = ct.id
 *   AND mc.company_id = cn.id
 *   AND mi.movie_id = t.id
 *   AND ml.movie_id = mk.movie_id
 *   AND ml.movie_id = mc.movie_id
 *   AND mk.movie_id = mc.movie_id
 *   AND ml.movie_id = mi.movie_id
 *   AND mk.movie_id = mi.movie_id
 *   AND mc.movie_id = mi.movie_id
 *   AND cn.name_pcode_nf = cn.name_pcode_sf;
 */
public class Query4 {

    public static WayangPlan getWayangPlan(String dataPath, Collection<LtMlTMkKMcCtCnMi> collector){

        TextFileSource companyNameText = new TextFileSource(dataPath + "company_name.csv", "UTF-8");
        TextFileSource companyTypeText = new TextFileSource(dataPath + "company_type.csv", "UTF-8");
        TextFileSource keywordText = new TextFileSource(dataPath + "keyword.csv", "UTF-8");
        TextFileSource linkTypeText = new TextFileSource(dataPath + "link_type.csv", "UTF-8");
        TextFileSource movieCompaniesText = new TextFileSource(dataPath + "movie_companies.csv", "UTF-8");
        TextFileSource movieInfoText = new TextFileSource(dataPath + "movie_info.csv", "UTF-8");
        TextFileSource movieKeywordText = new TextFileSource(dataPath + "movie_keyword.csv", "UTF-8");
        TextFileSource movieLinkText = new TextFileSource(dataPath + "movie_link.csv", "UTF-8");
        TextFileSource titleText = new TextFileSource(dataPath + "title.csv", "UTF-8");

        MapOperator<String, CompanyName> cnParser =
                new MapOperator<>(CompanyName::parseCsv, String.class, CompanyName.class);
        MapOperator<String, CompanyType> ctParser =
                new MapOperator<>(CompanyType::parseCsv, String.class, CompanyType.class);
        MapOperator<String, Keyword> kParser =
                new MapOperator<>(Keyword::parseCsv, String.class, Keyword.class);
        MapOperator<String, LinkType> ltParser =
                new MapOperator<>(LinkType::parseCsv, String.class, LinkType.class);
        MapOperator<String, MovieCompanies> mcParser =
                new MapOperator<>(MovieCompanies::parseCsv, String.class, MovieCompanies.class);
        MapOperator<String, MovieInfo> miParser =
                new MapOperator<>(MovieInfo::parseCsv, String.class, MovieInfo.class);
        MapOperator<String, MovieKeyword> mkParser =
                new MapOperator<>(MovieKeyword::parseCsv, String.class, MovieKeyword.class);
        MapOperator<String, MovieLink> mlParser =
                new MapOperator<>(MovieLink::parseCsv, String.class, MovieLink.class);
        MapOperator<String, Title> tParser =
                new MapOperator<>(Title::parseCsv, String.class, Title.class);

        FilterOperator<CompanyName> cnFilter =
                new FilterOperator<>(cn -> cn.countryCode() != "[pl]", CompanyName.class);
        FilterOperator<CompanyName> cnFilterTwo =
                new FilterOperator<>(cn -> cn.name().contains("Film") || cn.name().contains("Warner"), CompanyName.class);
        FilterOperator<CompanyName> cnFilterThree = new FilterOperator<>(cnFilterTwo);

        FilterOperator<CompanyType> ctFilter =
                new FilterOperator<>(ct -> ct.kind().equals("production companies"), CompanyType.class);

        FilterOperator<Keyword> kFilter =
                new FilterOperator<>(k -> k.keyword().equals("sequel"), Keyword.class);

        FilterOperator<LinkType> ltFilter =
                new FilterOperator<>(lt -> lt.link().contains("follow"), LinkType.class);

        FilterOperator<MovieCompanies> mcFilter =
                new FilterOperator<>(mc -> mc.note() == null, MovieCompanies.class);

        FilterOperator<MovieInfo> miFilter =
                new FilterOperator<>(mi ->
                        Arrays.asList("Sweden","Norway","Germany","Denmark",
                                "Swedish","Denish","Norwegian","German")
                                .contains(mi.info()),
                        MovieInfo.class);

        FilterOperator<Title> tFilter =
                new FilterOperator<>(t -> t.productionYear() >= 1950 && t.productionYear() <= 2000, Title.class);

        JoinOperator<LinkType, MovieLink, Integer> ltMlJoin =
                new JoinOperator<>(LinkType::id, MovieLink::linkTypeId,
                        LinkType.class, MovieLink.class, Integer.class);

        JoinOperator<Tuple2<LinkType, MovieLink>, Title, Integer> ltMlTJoin =
                new JoinOperator<>(ltMl -> ltMl.field1.movieId(),
                        Title::id,
                        ReflectionUtils.specify(Tuple2.class),
                        Title.class,
                        Integer.class);

        JoinOperator<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword, Integer> ltMlTMkJoin =
                new JoinOperator<>(ltMlT -> ltMlT.field1.id(),
                        MovieKeyword::movieId,
                        ReflectionUtils.specify(Tuple2.class),
                        MovieKeyword.class,
                        Integer.class);

        JoinOperator<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword, Integer> ltMlTMkKJoin =
                new JoinOperator<>(ltMlTMk -> ltMlTMk.field1.keywordId(),
                        Keyword::id,
                        ReflectionUtils.specify(Tuple2.class),
                        Keyword.class,
                        Integer.class);

        JoinOperator<Tuple2<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword>, MovieCompanies, Integer> ltMlTMkKMcJoin =
                new JoinOperator<>(ltMlTMkK -> ltMlTMkK.field0.field1.id(),
                        MovieCompanies::movieId,
                        ReflectionUtils.specify(Tuple2.class),
                        MovieCompanies.class,
                        Integer.class);

        JoinOperator<Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<LinkType, MovieLink>, Title>, MovieKeyword>, Keyword>, MovieCompanies>, CompanyType, Integer> ltMlTMkKMcCtJoin =
                new JoinOperator<>(ltMlTMkKMc -> ltMlTMkKMc.field1.companyTypeId(),
                        CompanyType::id,
                        ReflectionUtils.specify(Tuple2.class),
                        CompanyType.class,
                        Integer.class);

        JoinOperator<LtMlTMkKMcCt, CompanyName, Integer> ltMlTMkKMcCtCnJoin =
                new JoinOperator<>(ltMlTMkKMcCt -> ltMlTMkKMcCt.field0.field1.companyId(),
                        CompanyName::id,
                        ReflectionUtils.specify(LtMlTMkKMcCt.class),
                        CompanyName.class,
                        Integer.class);

        JoinOperator<Tuple2<LtMlTMkKMcCt, CompanyName>, MovieInfo, Integer> ltMlTMkKMcCtCnMiJoin =
                new JoinOperator<>(tuple ->
                                tuple.field0.field0.field0.field0.field0.field0.field1.movieId(),
                        MovieInfo::movieId,
                        ReflectionUtils.specify(Tuple2.class),
                        MovieInfo.class,
                        Integer.class);

        JoinOperator<LtMlTMkKMcCtCnMi, CompanyName, String> cnSelfJoin =
                new JoinOperator<>(tuple -> tuple.field0.field1.namePcodeNf(),
                        CompanyName::namePcodeSf,
                        ReflectionUtils.specify(LtMlTMkKMcCtCnMi.class),
                        CompanyName.class,
                        String.class);

        ReduceByOperator<LtMlTMkKMcCtCnMi, String> cnMin =
                new ReduceByOperator<>(
                        tuple -> tuple.field0.field1.name(),
                        (t1, t2) -> t1.field0.field1.name().compareTo(t2.field0.field1.name()) <= 0 ? t1 : t2,
                        String.class,
                        LtMlTMkKMcCtCnMi.class
                );

        ReduceByOperator<LtMlTMkKMcCtCnMi, String> ltMin =
                new ReduceByOperator<>(
                        tuple -> tuple.field0.field0.field0.field0.field0.field0.field0.field0.link(),
                        (t1, t2) -> t1.field0.field0.field0.field0.field0.field0.field0.field0.link()
                                .compareTo(t2.field0.field0.field0.field0.field0.field0.field0.field0.link()) <= 0 ? t1 : t2,
                        String.class,
                        LtMlTMkKMcCtCnMi.class
                );

        ReduceByOperator<LtMlTMkKMcCtCnMi, String> tMin =
                new ReduceByOperator<>(
                        tuple -> tuple.field0.field0.field0.field0.field0.field0.field1.title(),
                        (t1, t2) -> t1.field0.field0.field0.field0.field0.field0.field1.title()
                                .compareTo(t2.field0.field0.field0.field0.field0.field0.field1.title()) <= 0 ? t1 : t2,
                        String.class,
                        LtMlTMkKMcCtCnMi.class
                );

        LocalCallbackSink<LtMlTMkKMcCtCnMi> sink =
                LocalCallbackSink.createCollectingSink(
                        collector,
                        DataSetType.createDefaultUnchecked(LtMlTMkKMcCtCnMi.class)
                );

        // Connections
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

        ctParser.connectTo(0, ctFilter, 0);
        kParser.connectTo(0, kFilter, 0);
        ltParser.connectTo(0, ltFilter, 0);
        mcParser.connectTo(0, mcFilter, 0);
        miParser.connectTo(0, miFilter, 0);
        tParser.connectTo(0, tFilter, 0);

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

        ltMlTMkKMcCtCnJoin.connectTo(0, ltMlTMkKMcCtCnMiJoin, 0);
        miFilter.connectTo(0, ltMlTMkKMcCtCnMiJoin, 1);

        ltMlTMkKMcCtCnMiJoin.connectTo(0, cnSelfJoin, 0);
        cnFilterThree.connectTo(0, cnSelfJoin, 1);

        cnSelfJoin.connectTo(0, cnMin, 0);
        cnMin.connectTo(0, ltMin, 0);
        ltMin.connectTo(0, tMin, 0);
        tMin.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }

    private static class LtMlTMkKMcCt extends Tuple2<
            Tuple2<
                Tuple2<
                    Tuple2<
                        Tuple2<
                            Tuple2<LinkType, MovieLink>,
                            Title>,
                        MovieKeyword>,
                    Keyword>,
                MovieCompanies>,
            CompanyType> {}

    private static class LtMlTMkKMcCtCnMi extends Tuple2<
            Tuple2<LtMlTMkKMcCt, CompanyName>,
            MovieInfo> {}
}
