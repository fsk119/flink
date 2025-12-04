/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.openai;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.types.logical.ObjectRefType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.objectref.ObjectRef;

import com.openai.models.ChatModel;
import com.openai.models.ResponseFormatJsonObject;
import com.openai.models.ResponseFormatText;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionContentPart;
import com.openai.models.chat.completions.ChatCompletionContentPartImage;
import com.openai.models.chat.completions.ChatCompletionContentPartText;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionCreateParams.ResponseFormat;
import com.openai.models.chat.completions.ChatCompletionUserMessageParam;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** {@link AsyncPredictFunction} for OpenAI chat completion task. */
public class OpenAIChatModelFunction extends AbstractOpenAIModelFunction {
    private static final long serialVersionUID = 1L;

    public static final String ENDPOINT_SUFFIX = "chat/completions";

    public static final String STOP_SEPARATOR = ",";

    private final String model;
    private final String systemPrompt;
    private final Configuration config;
    private final int outputColumnIndex;

    public OpenAIChatModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        super(factoryContext, config);
        model = config.get(OpenAIOptions.MODEL);
        systemPrompt = config.get(OpenAIOptions.SYSTEM_PROMPT);
        this.config = Configuration.fromMap(config.toMap());
        validateSingleColumnSchema(
                factoryContext.getCatalogModel().getResolvedOutputSchema(),
                "output",
                Set.of(new VarCharType(VarCharType.MAX_LENGTH), new ObjectRefType()));
        this.outputColumnIndex = getOutputColumnIndex();
    }

    private int getOutputColumnIndex() {
        for (int i = 0; i < this.outputColumnNames.size(); i++) {
            String columnName = this.outputColumnNames.get(i);
            if (ErrorMessageMetadata.get(columnName) == null) {
                // Prior checks have guaranteed that there is one and only one physical output
                // column.
                return i;
            }
        }
        throw new IllegalArgumentException(
                "There should be one and only one physical output column. Actual columns: "
                        + this.outputColumnNames);
    }

    @Override
    protected String getEndpointSuffix() {
        return ENDPOINT_SUFFIX;
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncPredictInternal(String input) {
        ChatCompletionCreateParams.Builder builder =
                ChatCompletionCreateParams.builder()
                        .addSystemMessage(systemPrompt)
                        .addUserMessage(input)
                        .model(model);
        this.config.getOptional(OpenAIOptions.TEMPERATURE).ifPresent(builder::temperature);
        this.config.getOptional(OpenAIOptions.TOP_P).ifPresent(builder::topP);
        this.config
                .getOptional(OpenAIOptions.STOP)
                .ifPresent(x -> builder.stopOfStrings(Arrays.asList(x.split(STOP_SEPARATOR))));
        this.config.getOptional(OpenAIOptions.MAX_TOKENS).ifPresent(builder::maxTokens);
        this.config.getOptional(OpenAIOptions.PRESENCE_PENALTY).ifPresent(builder::presencePenalty);
        this.config.getOptional(OpenAIOptions.N).ifPresent(builder::n);
        this.config.getOptional(OpenAIOptions.SEED).ifPresent(builder::seed);
        this.config
                .getOptional(OpenAIOptions.RESPONSE_FORMAT)
                .ifPresent(x -> builder.responseFormat(x.getResponseFormat()));

        return client.chat().completions().create(builder.build()).handle(this::convertToRowData);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncPredictInternal(ObjectRef objectRef) {
        String base64Image;
        try (InputStream stream = objectRef.getAccessor().getInputStream();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                OutputStream base64Out = Base64.getEncoder().wrap(baos)) {
            copyBytes(stream, base64Out);
            base64Image = baos.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 组合为 data URL（注意 MIME 类型根据你的图片调整）
        String imageUrl = "data:image/jpeg;base64," + base64Image;

        ChatCompletionContentPart logoContentPart =
                ChatCompletionContentPart.ofImageUrl(
                        ChatCompletionContentPartImage.builder()
                                .imageUrl(
                                        ChatCompletionContentPartImage.ImageUrl.builder()
                                                .url(imageUrl)
                                                .build())
                                .build());
        ChatCompletionCreateParams createParams =
                ChatCompletionCreateParams.builder()
                        .model(model)
                        .maxCompletionTokens(2048)
                        .addUserMessageOfArrayOfContentParts(List.of(logoContentPart))
                        .build();

        //        ChatCompletionUserMessageParam.builder().
        //        ChatCompletionCreateParams.Builder builder =
        //                ChatCompletionCreateParams.builder()
        //                        .addSystemMessage(systemPrompt)
        //                        .addUserMessage(input)
        //                        .model(model);
        //
        // this.config.getOptional(OpenAIOptions.TEMPERATURE).ifPresent(builder::temperature);
        //        this.config.getOptional(OpenAIOptions.TOP_P).ifPresent(builder::topP);
        //        this.config
        //                .getOptional(OpenAIOptions.STOP)
        //                .ifPresent(x ->
        // builder.stopOfStrings(Arrays.asList(x.split(STOP_SEPARATOR))));
        //        this.config.getOptional(OpenAIOptions.MAX_TOKENS).ifPresent(builder::maxTokens);
        //
        // this.config.getOptional(OpenAIOptions.PRESENCE_PENALTY).ifPresent(builder::presencePenalty);
        //        this.config.getOptional(OpenAIOptions.N).ifPresent(builder::n);
        //        this.config.getOptional(OpenAIOptions.SEED).ifPresent(builder::seed);
        //        this.config
        //                .getOptional(OpenAIOptions.RESPONSE_FORMAT)
        //                .ifPresent(x -> builder.responseFormat(x.getResponseFormat()));

        return client.chat().completions().create(createParams).handle(this::convertToRowData);
    }

    private Collection<RowData> convertToRowData(
            ChatCompletion chatCompletion, Throwable throwable) {
        if (throwable != null) {
            return handleErrorsAndRespond(throwable);
        }

        return chatCompletion.choices().stream()
                .map(
                        choice -> {
                            GenericRowData rowData =
                                    new GenericRowData(this.outputColumnNames.size());
                            rowData.setField(
                                    this.outputColumnIndex,
                                    BinaryStringData.fromString(
                                            choice.message().content().orElse("")));
                            return rowData;
                        })
                .collect(Collectors.toList());
    }

    /**
     * The response format for Chat model function. It's an Enum representation for {@link
     * ResponseFormat}.
     */
    public enum ChatModelResponseFormat {
        TEXT("text") {
            @Override
            public ResponseFormat getResponseFormat() {
                return ResponseFormat.ofText(ResponseFormatText.builder().build());
            }
        },
        JSON_OBJECT("json_object") {
            @Override
            public ResponseFormat getResponseFormat() {
                return ResponseFormat.ofJsonObject(ResponseFormatJsonObject.builder().build());
            }
        };

        private final String value;

        ChatModelResponseFormat(String value) {
            this.value = value;
        }

        public abstract ResponseFormat getResponseFormat();

        @Override
        public String toString() {
            return value;
        }
    }
}
