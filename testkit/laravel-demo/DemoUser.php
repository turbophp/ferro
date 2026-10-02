<?php

namespace Illuminate\Tests\Integration\Database\FerroDemo;

use Illuminate\Foundation\Auth\User as Authenticatable;
use Illuminate\Notifications\Notifiable;

/** The demo app's user model — the stock Laravel 11 skeleton's `App\Models\User`, renamed. */
class DemoUser extends Authenticatable
{
    use Notifiable;

    protected $table = 'users';

    protected $fillable = ['name', 'email', 'password'];

    protected $hidden = ['password', 'remember_token'];
}
