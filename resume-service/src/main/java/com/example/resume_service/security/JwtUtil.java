package com.example.resume_service.security;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.springframework.stereotype.Component;
import java.util.Date;

@Component
public class JwtUtil {
    private final String SECRET = "mysecretkey12345678901234567890ab";

    public String generateToken(String email) {
        return Jwts.builder()
                .setSubject(email)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + 86400000L))
                .signWith(SignatureAlgorithm.HS256, SECRET.getBytes())  // ← .getBytes()
                .compact();
    }

    public String extractEmail(String token) {
        return Jwts.parser()
                .setSigningKey(SECRET.getBytes())  // ← .getBytes()
                .parseClaimsJws(token)
                .getBody()
                .getSubject();
    }
}