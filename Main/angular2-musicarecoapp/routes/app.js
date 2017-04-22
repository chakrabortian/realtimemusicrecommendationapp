var express = require('express');
var request = require('request');
var router = express.Router();

var  WebSocket = require('ws');







router.post('/acceptUserSurvey', function(req, res, next){
var ws = new WebSocket('ws://localhost:9000/acceptUserSurvey')

var username = req.body.username
var artist = req.body.artist
console.warn('acceptUserSurvey :' + username )
console.warn('acceptUserSurvey : ' + artist)

ws.on('open', function open() {
    console.log('acceptUserSurvey : sending to play')
  ws.send(username + '<SEP>' + artist);
});

ws.on('message', function incoming(data, flags) {
    console.log('acceptUserSurvey : received from play', data)
  res.send(data)
});


});


router.get('/existingUserReco*', function(req, res, next) {
var ws = new WebSocket('ws://localhost:9000/existingUserReco')

var username = req.query.username;

console.warn('existingUserReco :' + username )


ws.on('open', function open() {
    console.log('existingUserReco : sending to play')
  ws.send(username);
});

ws.on('message', function incoming(data, flags) {
    console.log('existingUserReco : received from play', data)
  
          res.send(data)
  
});

});


router.post('/lyricsBasedReco', function(req, res, next){
var ws = new WebSocket('ws://localhost:9000/lyricsBasedReco')

var username = req.body.username
var trackId = req.body.trackId

console.warn('lyricsBasedReco :' + username )
console.warn('lyricsBasedReco : ' + trackId)


ws.on('open', function open() {
    console.log('lyricsBasedReco : sending to play')
  ws.send(username + '<SEP>' + trackId);
});

ws.on('message', function incoming(data, flags) {
    console.log('lyricsBasedReco : received from play', data)
  res.send(data)
});

});


router.post('/similarArtist', function(req, res, next){
var ws = new WebSocket('ws://localhost:9000/similarArtist')

var username = req.body.username
var artist = req.body.artist

console.warn('similarArtist :' + username )
console.warn('similarArtist : ' + artist)


ws.on('open', function open() {
    console.log('similarArtist : sending to play')
  ws.send(username + '<SEP>' + artist + '<SEP>' + artist + '<SEP>' + artist);
});

ws.on('message', function incoming(data, flags) {
    console.log('similarArtist : received from play', data)
  res.send(data)
});

});




router.get('/', function (req, res, next) {
console.log("Request received and calling play f/w")
  

console.log('Rendering index.html now')
    res.render('index');
});

router.post('/signIn', function(req, res, next) {
    console.log(req.body)
    var userName = req.body.userName;
    var password = req.body.password;
        console.log(userName, password);
    request.post('http://localhost:9000/signInUser',{ json: { 'userName': userName, 'password' : password } }, function (error, response, body) {
        if (!error && response.statusCode == 200) {
            console.log(body)
            return res.json(body)
        } else {
            return res.json({status : 600 , message : 'KO'})
        }
    }

);

});

router.post('/signUp', function(req, res, next){

    var userName = req.body.userName;
    var password = req.body.password;

      request.post('http://localhost:9000/createUser',{ json: { 'userName': userName, 'password' : password } }, function (error, response, body){

        if (!error && response.statusCode == 200) {
            console.log(response.statusCode);
            return res.json(req.body);
        } else {
            console.log('error : ',response)
            return res.json({status : 600 , message : 'KO'});
        }

      }) 
        
});

router.get('/userArtist*', function(req, res, next){
        var userId = req.query.userId;
        request.get('http://localhost:9000/getUserArtist?userId='+userId, function(error, response, body){
            if (!error && response.statusCode == 200) {
                    console.log(body);
            return res.json(body);
            } else {
                return res.json({status : 600 , message : 'KO'});
            }
        });
});

router.get('/getArtistSongs*', function(req, res, next){
    var artistName = req.query.artistName;
    request.get('http://localhost:9000/getArtistSongs?artistName='+artistName, function(error, response, body){
        if (!error && response.statusCode == 200) {
            console.log(body);
            return res.json(body);
        } else {
            return res.json({status : 600 , message : 'KO'});
        }
    });
});

module.exports = router;
