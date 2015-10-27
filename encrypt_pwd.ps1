
function Enc_pwd
{
    $secure = Read-Host -AsSecureString -Prompt "Enter your password:"
    $usr = Read-Host -Prompt "Enter your username:"
    $bytes = ConvertFrom-SecureString $secure
    $bytes | Out-File ".\$usr"
}


Enc_pwd