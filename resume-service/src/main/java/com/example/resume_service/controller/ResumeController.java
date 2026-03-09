package com.example.resume_service.controller;

import com.example.resume_service.model.Resume;
import com.example.resume_service.producer.ResumeProducer;
import com.example.resume_service.repository.ResumeRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.interactive.action.PDAction;
import org.apache.pdfbox.pdmodel.interactive.action.PDActionURI;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotation;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationLink;
import org.apache.pdfbox.text.PDFTextStripper;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/resumes")
@RequiredArgsConstructor
public class ResumeController {

    private final ResumeRepository repository;
    private final ResumeProducer producer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping("/upload")
    public Resume upload(@RequestParam MultipartFile file) throws Exception {

        PDDocument doc = PDDocument.load(file.getInputStream());

        PDFTextStripper stripper = new PDFTextStripper();
        stripper.setSortByPosition(true);
        stripper.setAddMoreFormatting(true);
        String text = stripper.getText(doc);

        // Extract hyperlinks
        StringBuilder links = new StringBuilder("\nLinks:\n");
        for (PDPage page : doc.getPages()) {
            for (PDAnnotation annotation : page.getAnnotations()) {
                if (annotation instanceof PDAnnotationLink linkAnnotation) {
                    PDAction action = linkAnnotation.getAction();
                    if (action instanceof PDActionURI uriAction) {
                        links.append(uriAction.getURI()).append("\n");
                    }
                }
            }
        }
        doc.close();

        String fullText = cleanText(text + links);

        Resume resume = new Resume();
        resume.setRawText(fullText);
        resume.setStatus("PROCESSING");

        Resume saved = repository.save(resume);
        producer.send(saved.getId(), fullText);
        return saved;
    }

    @PutMapping("/{id}")
    public void updateParsed(@PathVariable Long id,
                             @RequestBody String parsedJson) throws Exception {

        Resume resume = repository.findById(id).orElseThrow();
        objectMapper.readTree(parsedJson); // validate JSON
        resume.setParsedJson(parsedJson);
        resume.setStatus("COMPLETED");
        repository.save(resume);
    }

    @GetMapping("/{id}")
    public Map<String, Object> get(@PathVariable Long id) throws Exception {

        Resume resume = repository.findById(id).orElseThrow();

        Map<String, Object> result = new HashMap<>();
        result.put("id", resume.getId());
        result.put("status", resume.getStatus());
        result.put("data", resume.getRawText());

//        if (resume.getParsedJson() != null) {
//            result.put("data", objectMapper.readValue(resume.getParsedJson(), Map.class));
//        } else {
//            result.put("data", null);
//        }

        return result;
    }

    private String cleanText(String text) {
        text = text.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
        text = text.replaceAll("[^\\p{Print}\r\n\t]", " ");
        text = text.replaceAll("-\\r?\\n", "");
        text = text.replaceAll("\\r\\n", "\n");
        text = text.replaceAll(" +", " ");
        return text.trim();
    }
}