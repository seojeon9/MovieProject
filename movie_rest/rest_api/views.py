# from django.shortcuts import render
from datetime import date, datetime, timedelta
from django.contrib.auth.models import User, Group
from rest_framework import viewsets, permissions
from rest_api.models import *
from rest_api.serializers import *
from django.http import *
from drf_yasg.utils import swagger_auto_schema
from drf_yasg.openapi import Parameter, IN_QUERY, TYPE_STRING


# Create your views here.
def get_queryset_by_date(model, query_params):
    if ~('start_date' in query_params) and ~('end_date' in query_params):
        queryset = model.objects.filter(
            std_day__gt=(datetime.today() - timedelta(7)))

    if 'start_date' in query_params and 'end_date' in query_params:
        start_date = query_params['start_date']
        end_date = query_params['end_date']
        queryset = model.objects.filter(std_day__range=(
            date.fromisformat(start_date), date.fromisformat(end_date)))

    if 'start_date' in query_params:
        start_date = query_params['start_date']
        queryset = model.objects.filter(std_day__gt=start_date)

    if 'end_date' in query_params:
        end_date = query_params['end_date']
        queryset = model.objects.filter(std_day__lt=end_date)

    return queryset

# SAMPLE
class CoFacilityViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Actor.objects.all()
    serializer_class = MovieActorSerializers
    permission_classes = [permissions.IsAuthenticated]

<<<<<<< HEAD
    @swagger_auto_schema(
        operation_summary="인구밀도와 10만명당 코로나 발생 환자 간의 상관관계",
=======
#     @swagger_auto_schema(
#         operation_summary="인구밀도와 10만명당 코로나 발생 환자 간의 상관관계",
#         operation_description="""시작날짜와 끝날짜를 모두 생략하면 최근 1주일 데이터를 반환합니다. <br>
#         시작날짜만 입력하면 시작날짜 이후의 데이터를 반환합니다.<br>
#         끝날짜만 입력하면 끝날짜 이전 데이터를 반환합니다.<br>
#          """,
#         manual_parameters=[
#             Parameter("start_date", IN_QUERY, type=TYPE_STRING,
#                       description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
#             Parameter("end_date", IN_QUERY, type=TYPE_STRING,
#                       description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
#         ],
#     )
#     def list(self, request):
#         query_params = request.query_params
#         queryset = get_queryset_by_date(CoFacility, query_params)
#         print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
#         serializers = self.get_serializer(queryset, many=True)
#         return HttpResponse(serializers.data)

class MovieActorViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Actor.objects.all()
    serializer_class = MovieActorSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="영화코드 별 영화이름, 배우이름, 흥행등급 목록 반환",
>>>>>>> 7f3deed1c6f48f6c4693305c8b300cc397a91a6c
        operation_description="""시작날짜와 끝날짜를 모두 생략하면 최근 1주일 데이터를 반환합니다. <br>
        시작날짜만 입력하면 시작날짜 이후의 데이터를 반환합니다.<br>
        끝날짜만 입력하면 끝날짜 이전 데이터를 반환합니다.<br>
         """,
        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
<<<<<<< HEAD
        queryset = get_queryset_by_date(CoFacility, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HttpResponse(serializers.data)
=======
        queryset = get_queryset_by_date(Actor, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieCompanyViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Company.objects.all()
    serializer_class = MovieCompanySerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="영화코드 별 영화사이름, 영화사종류, 흥행등급 목록 반환",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(Company, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieGenreViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Genre.objects.all()
    serializer_class = MovieGenreSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="영화코드 별 장르이름, 흥행등급 목록 반환",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(Genre, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Movie.objects.all()
    serializer_class = MovieSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary=" ",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(Movie, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieAudiViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = MovieAudi.objects.all()
    serializer_class = MovieAudiSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary=" ",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(MovieAudi, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieHitViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = MovieHit.objects.all()
    serializer_class = MovieHitSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary=" ",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(MovieHit, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieRankViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = MovieRank.objects.all()
    serializer_class = MovieRankSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary=" ",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(MovieRank, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieSalesViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = MovieSales.objects.all()
    serializer_class = MovieSalesSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary=" ",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(MovieSales, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieScoreViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = MovieScore.objects.all()
    serializer_class = MovieScoreSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary=" ",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(MovieScore, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieScrnViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = MovieScrn.objects.all()
    serializer_class = MovieScrnSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary=" ",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(MovieScrn, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieSearchViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = MovieSearch.objects.all()
    serializer_class = MovieSearchSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary=" ",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(MovieSearch, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)


class MovieShowViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = MovieShow.objects.all()
    serializer_class = MovieShowSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary=" ",
        operation_description=" ",

        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )
    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(MovieShow, query_params)
        print('params : >>>>>>>>>>>>>>>>>>>> ', query_params)
        serializers = self.get_serializer(queryset, many=True)
        return HTTPResponse(serializers.data)
>>>>>>> 7f3deed1c6f48f6c4693305c8b300cc397a91a6c
