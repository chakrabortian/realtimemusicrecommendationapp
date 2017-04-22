import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent } from "./app.component";
import {HeaderComponent} from './header/header.component';
import {HomeComponent} from './home/home.component';
import {LoginComponent} from './login/login.component';
import {routing} from './app.routing'
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import {RecommendationsComponent} from './recommendation/reco.component';
import {ArtistpageComponent} from './artist/artist.component';
import {SignupComponent} from './signup/signup.component'
import {UserPreferenceComponent} from './preference/userpreference.component'
import {ColdStartComponent} from './recommendation/coldstart.component'

@NgModule({
    declarations: [
        AppComponent, HeaderComponent, HomeComponent, LoginComponent, RecommendationsComponent
        ,ArtistpageComponent, SignupComponent, UserPreferenceComponent, ColdStartComponent
    ],
    imports: [BrowserModule,
    FormsModule,
    HttpModule,
    ReactiveFormsModule,
    routing],
    bootstrap: [AppComponent]
})
export class AppModule {

}