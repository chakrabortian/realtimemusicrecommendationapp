import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs/Rx';
import { FormArray, FormGroup, FormControl, Validators, FormBuilder, FormsModule, ReactiveFormsModule } from '@angular/forms';
import {LoginService} from './login.service'
import {User} from '../user'


@Component({
    selector: 'login-form',
    templateUrl: './login.component.html',
    providers : [LoginService]
})
export class LoginComponent implements OnInit{
    private loginForm: FormGroup;
    private isNew: boolean;
    private user : User = new User('', '');

    //private user: User = new User('','');
   
     constructor(private formBuilder: FormBuilder,
        private activatedRoute: ActivatedRoute,
        private router: Router,
        private loginService : LoginService) { }
    ngOnInit(){
        this.initForm();        
    }
    private initForm(){
        let username = '';
        let password = '';
        
     //   username = this.user.username;
     //   password = this.user.password;

        this.loginForm = this.formBuilder.group({
            username: ['', Validators.required],
            password: ['', Validators.required]
        })
    }
    onSubmit(){
        var uname = this.loginForm.controls['username'].value;
        var password = this.loginForm.controls['password'].value;
        console.log(uname, password)
        console.log('Sending request to node and play')
        var sub = this.loginService.signInUser(uname, password).subscribe((u : User) => {
            this.user = u
            console.log('this.user : ', this.user)
            this.router.navigate(['/recommendation', this.user.userName]);
        });
        
        
        
    }
    
    }