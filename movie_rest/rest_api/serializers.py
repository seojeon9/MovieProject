from rest_framework import serializers
from rest_api.models import Actor, Company, Genre, Movie

class MovieActorSerializers(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = Actor
        fields = ['actor_id', 'movie_code', 'movie_name', 'actor_name','hit_grade']


class MovieCompanySerializers(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = Company
        fields = ['company_id', 'movie_code', 'company_name', 'company_type','hit_grade']


class MovieGenreSerializers(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = Genre
        fields = ['genre_id', 'movie_code', 'genre_name', 'hit_grade']


class MovieMovieSerializers(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = Movie
        fields = ['movie_code', 'movie_name', 'show_tm'
                    ,'open_date','type_name','nation_name'
                    ,'director','watch_grade_name','dist_name'
                    ,'peak_yn','genre','hit_grade']
