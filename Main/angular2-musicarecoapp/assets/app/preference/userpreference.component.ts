import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs/Rx';
import { FormArray, FormGroup, FormControl, Validators, FormBuilder, FormsModule, ReactiveFormsModule } from '@angular/forms';
import {LoginService} from '../login/login.service'


@Component({
    selector: 'user-preference',
    templateUrl: './userpreference.component.html',
    providers : [LoginService]
})
export class UserPreferenceComponent  {
    
    private artistList : string[] = ['The Rolling Stones', 'Pink Floyd', 'The Beatles', 'Taylor Swift', 'Usher',
    'Natasha Bedingfield', 'Savage Garden', 'Soundgarden', 'Nightwish', 'Shakira', 'Coldplay','Metallica',
    'Muse', 'Britney Spears']
    private counter = 0
    private finalList : string[] = []
    private username = ''
   
     constructor(
        private activatedRoute: ActivatedRoute,
        private router: Router,
        private loginService : LoginService) {

            this.username = this.activatedRoute.snapshot.params['id'];

         }

    
    onClick(artist : string) {
        if(this.counter === 3) {
            alert("Not allowed to Select more than 3 artist")
            } else {
                this.counter++;
                this.finalList.push(artist);
        }
    }
    

    onSubmit(){
        var artist = this.finalList.join("<SEP>");
        console.log(artist)
        console.log(this.username)
        this.router.navigate(['/coldstart', this.username, artist]);
        

        
        
    }
    
    }