package com.example.resume_service.controller;

import com.example.resume_service.model.Resume;
import com.example.resume_service.model.User;
import com.example.resume_service.repository.ResumeRepository;
import com.example.resume_service.repository.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.interactive.action.PDAction;
import org.apache.pdfbox.pdmodel.interactive.action.PDActionURI;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotation;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationLink;
import org.apache.pdfbox.text.PDFTextStripper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/resumes")
@RequiredArgsConstructor
public class ResumeController {

    private final ResumeRepository repository;
    private final UserRepository userRepository;
    private final RestTemplate restTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${fastapi.url:http://host.docker.internal:8000}")
    private String fastApiUrl;

    // 🔐 AUTHENTICATED UPLOAD
    @PostMapping("/upload")
    public Resume upload(@RequestParam MultipartFile file,
                         Authentication authentication) throws Exception {

        // 🔥 1. GET LOGGED-IN USER
        String email = authentication.getName();
        User user = userRepository.findByEmail(email)
                .orElseThrow(() -> new RuntimeException("User not found"));

        String fullText;

        // ── SAFE PDF HANDLING ─────────────────────────────
        try (InputStream is = file.getInputStream();
             PDDocument doc = PDDocument.load(is)) {

            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setSortByPosition(true);

            String text = stripper.getText(doc);

            StringBuilder links = new StringBuilder("\nLinks:\n");
            for (PDPage page : doc.getPages()) {
                for (PDAnnotation annotation : page.getAnnotations()) {
                    if (annotation instanceof PDAnnotationLink link) {
                        PDAction action = link.getAction();
                        if (action instanceof PDActionURI uri) {
                            links.append(uri.getURI()).append("\n");
                        }
                    }
                }
            }

            fullText = cleanText(text + links);
        }

        // ── SAVE INITIAL STATE ───────────────────────────
        Resume resume = new Resume();
        resume.setRawText(fullText);
        resume.setStatus("PROCESSING");

        // 🔥 IMPORTANT: LINK RESUME TO USER
        resume.setUser(user);

        Resume saved = repository.save(resume);

        // ── BUILD PAYLOAD ────────────────────────────────
        Map<String, Object> payload = new HashMap<>();
        payload.put("resume_id", saved.getId());
        payload.put("text", fullText);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Map<String, Object>> request =
                new HttpEntity<>(payload, headers);

        ResponseEntity<Map> response;

        try {
            response = restTemplate.exchange(
                    fastApiUrl + "/parse",
                    HttpMethod.POST,
                    request,
                    Map.class
            );
        } catch (Exception e) {
            saved.setStatus("FAILED");
            repository.save(saved);
            throw new RuntimeException("FastAPI unreachable: " + e.getMessage());
        }

        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
            saved.setStatus("FAILED");
            repository.save(saved);
            throw new RuntimeException("FastAPI parse failed");
        }

        Object parsed = response.getBody().get("parsed");

        String parsedJson = objectMapper.writeValueAsString(parsed);

        saved.setParsedJson(parsedJson);
        saved.setStatus("COMPLETED");
        repository.save(saved);

        return saved;
    }

    // 🔐 GET ONLY USER'S OWN RESUME
    @GetMapping("/{id}")
    public Map<String, Object> get(@PathVariable Long id,
                                   Authentication authentication) {

        String email = authentication.getName();

        Resume resume = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Resume not found"));

        // 🔥 SECURITY CHECK
        if (!resume.getUser().getEmail().equals(email)) {
            throw new RuntimeException("Unauthorized access");
        }

        Map<String, Object> result = new HashMap<>();
        result.put("id", resume.getId());
        result.put("status", resume.getStatus());
        result.put("data", resume.getRawText());
        result.put("parsed", resume.getParsedJson());

        return result;
    }

    // 🔐 UPDATE ONLY USER'S OWN RESUME
    @PutMapping("/{id}")
    public void updateParsed(@PathVariable Long id,
                             @RequestBody String parsedJson,
                             Authentication authentication) throws Exception {

        String email = authentication.getName();

        Resume resume = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Resume not found"));

        if (!resume.getUser().getEmail().equals(email)) {
            throw new RuntimeException("Unauthorized access");
        }

        objectMapper.readTree(parsedJson);

        resume.setParsedJson(parsedJson);
        resume.setStatus("COMPLETED");

        repository.save(resume);
    }

    // 🧹 CLEANER
    private String cleanText(String text) {
        text = text.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
        text = text.replaceAll("[^\\p{Print}\r\n\t]", " ");
        text = text.replaceAll("-\\r?\\n", "");
        text = text.replaceAll("\\r\\n", "\n");
        text = text.replaceAll(" +", " ");
        return text.trim();
    }
}