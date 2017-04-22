import { Component, OnInit, OnChanges } from '@angular/core';
import { ActivatedRoute, Router } from "@angular/router";
import {LoginService} from '../login/login.service';
import {SongInfo} from '../songInfo';
import {StorageService} from '../storage.service';

@Component({
  selector: 'app-artistpage',
  templateUrl: './artist.component.html',
  providers : [LoginService]
  
})
export class ArtistpageComponent implements OnInit , OnChanges {
  private artistName:string;
  private userName : string;
  private songNames : string[] = []
  private trackIds : string[] = []
  private songIds : string[] = []
  private similarArtists : string[] = []
  private clickedArtist : string;
 //  songList:string[] = ['The Raven that refused to swing', 'Deform to form a bar', 'Outsurgentes', 'Luminol Maxima', 'Ride Home', 'index.html', "You're welcome", "Happiness IV: A New Happiness"];
  constructor(private route: ActivatedRoute,
    private router: Router,
                private loginService : LoginService,
                private storageService : StorageService) { 
  
  
                  this.initialize()

  }

  initialize() {


    this.userName = this.route.snapshot.params['id'];
    this.artistName = this.route.snapshot.params['artistName'];


 this.loginService.getArtistSongs(this.artistName)
          .subscribe(s => {
              console.log('songs : ' + s)

       var songs : string[] = JSON.parse(s).songs.split('<SEP>')
       console.log('songs : ', songs);
       songs.forEach(sng => {

         var splitted = sng.split("<SONGINFO>")
        
        
          this.songNames.push(splitted[2])
          this.trackIds.push(splitted[0])
          this.songIds.push(splitted[1])
          
         
       })
       
     })


this.loginService.similarArtist(this.userName, this.artistName).subscribe(r => {
  console.log("RECEIVE RECO FOR SIMILAR ARTIST " + r)
      this.similarArtists = r.artist.split('<SEP>')

})



  }

  ngOnChanges() {

  }


  ngOnInit() {
  
  }

  onClick(i, song) {
    this.storageService.addSong(this.trackIds[i])
    var sessionTracks = this.storageService.getSession()
    
   this.loginService.lyricsBasedReco(this.userName, this.trackIds.join("<SEP>")).subscribe(a => {
     console.log("RECEIVED OUTPUT FOR LAST : " + a)
   })


  }

  findArtistSongs(artistName : string) {
    this.clickedArtist = artistName
    console.log('Redirecting to artist page : ' + artistName) 
    this.router.navigate(['/artist', this.userName, artistName]);

  }

}
