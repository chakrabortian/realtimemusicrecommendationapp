import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs/Rx';
import {LoginService} from '../login/login.service';
import {WebSocketService} from '../websocket.service';
import { Observable, Subject } from 'rxjs/Rx'

@Component({
  selector: 'app-coldstart',
  templateUrl: './coldstart.component.html',
  providers : [LoginService, WebSocketService]
  
})
export class ColdStartComponent implements OnInit {
private  username;
 private artist;
 private coldstartArtistRecommendation : string[];


 constructor(private activatedRoute: ActivatedRoute,
    private router: Router,
    private loginService : LoginService,
    private websocketService : WebSocketService) {

 }

 ngOnInit() {
     this.username = this.activatedRoute.snapshot.params['id']
     this.artist = this.activatedRoute.snapshot.params['artist']
     console.log('p : ' , this.username)
     
     
    this.getExistingUserRecommendation()
  }

  getExistingUserRecommendation() {
      
      this.loginService.acceptUserSurvey(this.username, this.artist).subscribe(r => {
        console.log("****** reco received : ****** " + r)

        var artist : string[] = r.artist.split('<SEP>')
       console.log('artist : ' + artist)

       this.coldstartArtistRecommendation = artist
      });
      
  }

 onClick(artistName) {
    this.router.navigate(['/artist', this.username, artistName]);
  }
}