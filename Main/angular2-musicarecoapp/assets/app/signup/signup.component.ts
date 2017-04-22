import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs/Rx';
import { FormArray, FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { User } from '../user';
import {LoginService} from '../login/login.service'

@Component({
    selector: 'signup-form',
    templateUrl: './signup.component.html',
    providers : [LoginService]
})
export class SignupComponent implements OnInit {
    private signupForm: FormGroup;
    private isNew: boolean;
    private user: User = new User('', '');
    
    

    constructor(private formBuilder: FormBuilder,
        private activatedRoute: ActivatedRoute,
        private router: Router,
        private loginService : LoginService ) { }

    ngOnInit() {
        this.initForm();
    }

    private initForm() {
        let username = '';
        let password = '';

        const signupCredentials = new FormGroup({
            username: new FormControl('', Validators.required),
            password: new FormControl('', Validators.required)
        })

        

        this.signupForm = this.formBuilder.group({
            username: [username, Validators.required],
            password: [password, Validators.required]
        })
    }

    onSubmit() {

        var uname = this.signupForm.controls['username'].value;
        var password = this.signupForm.controls['password'].value;
         console.log(uname, password)
        console.log('Sending request to node and play')
        this.loginService.signUpUser(uname, password).subscribe(r => {
            console.log('response => ' +r)
        })
        this.router.navigate(['/userpreference', uname])

    }
}

