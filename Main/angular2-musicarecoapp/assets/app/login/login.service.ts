import {Injectable} from '@angular/core';
import {Headers, Http, Response} from '@angular/http';
import 'rxjs/Rx';

@Injectable()
export class LoginService {

    constructor(private http : Http) {}


    signUpUser(username : string, password : string) {
            var body = JSON.stringify({'userName' : username, 'password' : password});
            console.log(body);
            const headers = new Headers({
                'Content-Type' : 'application/json'
            });
            return this.http.post('http://localhost:3000/signUp', body, {headers : headers})
            .map(m => m.json());
    }

    signInUser(username : string, password : string) {
            var body = JSON.stringify({'userName' : username, 'password' : password});
            console.log(body);
            const headers = new Headers({
                'Content-Type' : 'application/json'
            });
            return this.http.post('http://localhost:3000/signIn', body, {headers : headers})
            .map(m => m.json());
    }

    getUserArtist(username : string)  {
            return this.http.get('http://localhost:3000/userArtist?userId='+username)
            .map(m => m.json());
    }

    getArtistSongs(artistName : string) {
            return this.http.get('http://localhost:3000/getArtistSongs?artistName='+artistName)
            .map(m => m.json());
    }


    acceptUserSurvey(username : string, artist : string) {
              var body = JSON.stringify({'username' : username, 'artist' : artist});
            
            const headers = new Headers({
                'Content-Type' : 'application/json'
            });
            return this.http.post('http://localhost:3000/acceptUserSurvey', body, {headers : headers})
            .map(m => m.json());
    }


    existingUserReco(username : string) {
            return this.http.get('http://localhost:3000/existingUserReco?username='+username)
            .map(m => m.json());
    }

    lyricsBasedReco(username : string, trackId : string) {
                var body = JSON.stringify({'username' : username, 'trackId' : trackId});
            
            const headers = new Headers({
                'Content-Type' : 'application/json'
            });
            return this.http.post('http://localhost:3000/lyricsBasedReco', body, {headers : headers})
            .map(m => m.json());
    }

    similarArtist(username : string, artist : string) {
                var body = JSON.stringify({'username' : username, 'artist' : artist});
            
            const headers = new Headers({
                'Content-Type' : 'application/json'
            });
            return this.http.post('http://localhost:3000/similarArtist', body, {headers : headers})
            .map(m => m.json());
    }

    
}