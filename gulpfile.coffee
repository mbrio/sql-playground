gulp = require 'gulp'
play = require './index'

gulp.task 'play', -> play()
gulp.task 'default', ['play'], -> gulp.watch [play.fileName], ['play']
