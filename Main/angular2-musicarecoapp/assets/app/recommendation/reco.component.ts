import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs/Rx';
import {LoginService} from '../login/login.service';
import {WebSocketService} from '../websocket.service';
import { Observable, Subject } from 'rxjs/Rx'

@Component({
  selector: 'app-recommendations',
  templateUrl: './reco.component.html',
  providers : [LoginService, WebSocketService]
  
})
export class RecommendationsComponent implements OnInit {
private  username;
 private artistList: string[];
 private existingUserArtistRecommendation : string[];


 constructor(private activatedRoute: ActivatedRoute,
    private router: Router,
    private loginService : LoginService,
    private websocketService : WebSocketService) {

 }

 ngOnInit() {
     this.username = this.activatedRoute.snapshot.params['id']
     console.log('p : ' , this.username)
     this.loginService.getUserArtist(this.username)
     .subscribe(s => {
       var artist : string[] = JSON.parse(s).artist.split('<SEP>')
       console.log('artist : ' + artist)
       this.artistList = artist
     })
     
    this.getExistingUserRecommendation()
  }

  getExistingUserRecommendation() {
      
      this.loginService.existingUserReco(this.username).subscribe(r => {
        console.log("****** reco received : ****** " + r)
        var artist : string[] = r.artist.split('<SEP>')
       console.log('artist : ' + artist)
       this.existingUserArtistRecommendation = artist
      });
      
  }

 onClick(artistName) {
    this.router.navigate(['/artist', this.username, artistName]);
  }
}