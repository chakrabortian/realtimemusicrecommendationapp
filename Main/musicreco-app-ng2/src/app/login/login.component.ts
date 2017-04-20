import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs/Rx';
import { FormArray, FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';
import { User } from '../user';
import { UserService } from '../user.service';

@Component({
    selector: 'login-form',
    templateUrl: './login.component.html'
})

export class LoginComponent implements OnInit{
    private loginForm: FormGroup;
    private isNew: boolean;
    private user: User = new User('','');
    private userService: UserService;

     constructor(private formBuilder: FormBuilder,
        private activatedRoute: ActivatedRoute,
        private router: Router ) { }


    ngOnInit(){
        this.initForm();        
    }

    private initForm(){
        let username = '';
        let password = '';

        const userCredentials = new FormGroup({
            username: new FormControl('', Validators.required),
            password: new FormControl('', Validators.required)
        })

        username = this.user.username;
        password = this.user.password;

        this.loginForm = this.formBuilder.group({
            username: [username, Validators.required],
            password: [password, Validators.required]
        })
    }

    onSubmit(){
        const newUser = this.loginForm.value;
        if(this.isNew){
            this.userService.addUser(newUser);
        }
        this.router.navigate(['/recommendations'])
        }
    }

    