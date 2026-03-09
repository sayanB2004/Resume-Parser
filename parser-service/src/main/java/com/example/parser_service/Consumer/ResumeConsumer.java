package com.example.parser_service.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.springframework.http.HttpEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
public class ResumeConsumer {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    @Async
    @KafkaListener(topics = "resume-topic", groupId = "parser-group")
    public void consume(String message) {
        try {
            System.out.println("Kafka message received");

            String[] parts = message.split("\\|\\|", 2);
            if (parts.length < 2) {
                System.err.println("Invalid message format");
                return;
            }

            Long resumeId = Long.parseLong(parts[0]);
            String rawText = parts[1];

            // Parse resume text into structured JSON
            Map<String, Object> parsedJson = parseResume(rawText);

            System.out.println("Parsed JSON: " + objectMapper.writeValueAsString(parsedJson));

            // Send parsed JSON to resume-service via REST API
            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(parsedJson));
            restTemplate.put(
                    "http://resume-service:8081/api/resumes/" + resumeId,
                    request
            );



            System.out.println("Resume updated successfully! resumeId=" + resumeId);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // --- Parsing Logic ---

    private String cleanText(String text) {
        text = text.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
        text = text.replaceAll("[^\\p{Print}\r\n\t]", " ");
        text = text.replaceAll(" +", " ");
        return text.trim();
    }

    private Map<String, String> extractSections(String text) {
        Map<String, String> sections = new HashMap<>();
        Pattern pattern = Pattern.compile(
                "(?i)(Education|Skills|Projects|Certifications|Achievements)\\s*[:\\n]\\s*(.*?)(?=\\n[A-Z][a-z]+\\s*[:\\n]|$)",
                Pattern.DOTALL
        );
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            sections.put(matcher.group(1).toLowerCase(), matcher.group(2).trim());
        }
        return sections;
    }

    private Map<String, String> extractPersonalInfo(String text) {
        Map<String, String> info = new HashMap<>();

        String[] lines = text.split("\n");
        if (lines.length > 0) info.put("name", lines[0].trim());

        Matcher emailMatcher = Pattern.compile("[\\w._%+-]+@[\\w.-]+\\.[a-zA-Z]{2,6}").matcher(text);
        if (emailMatcher.find()) info.put("email", emailMatcher.group());

        Matcher phoneMatcher = Pattern.compile("\\+?\\d[\\d\\s-]{7,}\\d").matcher(text);
        if (phoneMatcher.find()) info.put("phone", phoneMatcher.group().replaceAll("\\s+", ""));

        return info;
    }

    private Map<String, Object> parseResume(String text) {
        text = cleanText(text);

        Map<String, String> sections = extractSections(text);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("personalInfo", extractPersonalInfo(text));
        result.put("education", parseEducationClean(sections.getOrDefault("education", "")));
        result.put("skills", parseSkillsClean(sections.getOrDefault("technical skills", sections.getOrDefault("skills", ""))));
        result.put("projects", parseProjectsClean(sections.getOrDefault("professional projects", sections.getOrDefault("projects", ""))));
        result.put("certifications", parseList(sections.getOrDefault("certifications & training", sections.getOrDefault("certifications", ""))));
        result.put("achievements", parseList(sections.getOrDefault("achievements", "")));
        result.put("links", extractLinks(text));

        return result;
    }

    private List<Map<String, String>> parseEducationClean(String text) {
        List<Map<String, String>> eduList = new ArrayList<>();
        // Split by double line breaks to detect blocks
        String[] blocks = text.split("\n\\s*\n");
        for (String block : blocks) {
            if (block.isBlank()) continue;
            Map<String, String> edu = new HashMap<>();
            String[] lines = block.split("\n");
            if (lines.length > 0) edu.put("institute", lines[0].trim());
            if (lines.length > 1) edu.put("degree", lines[1].trim());
            // Optional: try to find year in block
            Matcher yearMatcher = Pattern.compile("\\b(19|20)\\d{2}\\b").matcher(block);
            if (yearMatcher.find()) edu.put("year", yearMatcher.group());
            eduList.add(edu);
        }
        return eduList;
    }

    private Map<String, List<String>> parseSkillsClean(String text) {
        Map<String, List<String>> skills = new LinkedHashMap<>();
        String[] categories = {"Programming Languages", "Frontend", "Backend & Systems", "Databases", "Cloud & DevOps", "Tools", "Practices", "Core Competencies"};
        for (String cat : categories) {
            Matcher m = Pattern.compile("(?i)" + cat + "[:\\s]+(.+?)(?=\\n[A-Z]|$)", Pattern.DOTALL).matcher(text);
            List<String> skillList = new ArrayList<>();
            if (m.find()) {
                for (String s : m.group(1).split("[,\\n]")) {
                    if (!s.isBlank()) skillList.add(s.trim());
                }
            }
            skills.put(cat, skillList);
        }
        return skills;
    }

    private List<Map<String, String>> parseProjectsClean(String text) {
        List<Map<String, String>> projects = new ArrayList<>();
        String[] blocks = text.split("\n\\s*\n");
        for (String block : blocks) {
            if (block.isBlank()) continue;
            String[] lines = block.split("\n");
            if (lines.length > 0) {
                Map<String, String> p = new HashMap<>();
                p.put("name", lines[0].trim());
                StringBuilder desc = new StringBuilder();
                for (int i = 1; i < lines.length; i++) {
                    desc.append(lines[i].trim()).append(" ");
                }
                p.put("description", desc.toString().trim());
                projects.add(p);
            }
        }
        return projects;
    }

    private List<String> parseList(String text) {
        List<String> list = new ArrayList<>();
        for (String item : text.split("[\n,]")) {
            if (!item.isBlank()) list.add(item.trim());
        }
        return list;
    }

    private Map<String, String> extractLinks(String text) {
        Map<String, String> links = new LinkedHashMap<>();
        Pattern p = Pattern.compile("(https?://\\S+|mailto:\\S+)", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(text);
        while (m.find()) {
            String link = m.group();
            if (link.contains("linkedin")) links.put("linkedin", link);
            else if (link.contains("github")) links.put("github", link);
            else if (link.contains("leetcode")) links.put("leetcode", link);
            else if (link.startsWith("mailto:")) links.put("email", link.replace("mailto:", ""));
        }
        return links;
    }
}