"""app URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from django.urls import include, path
from django.conf.urls.static import static
from django.conf import settings
from rest_framework import routers
from rest_api import views as rest_view
from drf_yasg import openapi
from drf_yasg.views import get_schema_view
# from . import views


# router = routers.DefaultRouter()
# router.register(r'movie/movie_actor', rest_view.MovieAudiViewSet)


<<<<<<< HEAD
router = routers.DefaultRouter()


router.register(r'movie/movie_actor', rest_view.MovieActorSerializers)

router.register(r'movie/movie_company', rest_view.MovieCompanySerializers)

router.register(r'movie/movie_genre', rest_view.MovieGenreSerializers)

router.register(r'movie/movie_movie', rest_view.MovieMovieSerializers)

router.register(r'movie/movie_movie_audi', rest_view.MovieMovieAudiSerializers)

router.register(r'movie/movie_moviehit', rest_view.MovieMovieHitSerializers)

router.register(r'movie/movie_movie_rank', rest_view.MovieMovieRankSerializers)

router.register(r'movie/movie_movie_sales', rest_view.MovieMovieSalesSerializers)

router.register(r'movie/movie_movie_score', rest_view.MovieMovieScoreSerializers)

router.register(r'movie/movie_movie_scrn', rest_view.MovieMovieScrnSerializers)

router.register(r'movie/movie_movie_search', rest_view.MovieMovieSearchSerializers)

router.register(r'movie/movie_movie_show', rest_view.MovieMovieShowSerializers)

=======
>>>>>>> 7f3deed1c6f48f6c4693305c8b300cc397a91a6c


urlpatterns = [
    # path('admin/', admin.site.urls),
    path('', include(router.urls)),
]
